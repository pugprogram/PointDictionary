#include "PointDictionary.h"
#include <numeric>
#include <cmath>
#include <base/sort.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySource.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int TYPE_MISMATCH;
        extern const int BAD_ARGUMENTS;
        extern const int UNSUPPORTED_METHOD;
    }
    IPointDictionary::IPointDictionary(
        const StorageID& dict_id_,
        const DictionaryStructure& dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        Configuration configuration_)
        : IDictionary(dict_id_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
        , configuration(configuration_)
    {
        setup();
        loadData();
        calculateBytesAllocated();
    }



    void IPolygonDictionary::convertKeyColumns(Columns& key_columns, DataTypes& key_types) const
    {
        if (key_columns.size() != 2)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "Dictionary {} key lookup structure does not match, expected two columns of coordinates with type Float64",
                getFullName());
        auto float_64_type = std::make_shared<DataTypeFloat64>();
        size_t key_types_size = key_types.size();
        for (size_t key_type_index = 0; key_type_index < key_types_size; ++key_type_index)
        {
            auto& key_type = key_types[key_type_index];
            if (float_64_type->equals(*key_type))
                continue;
            auto& key_column_to_cast = key_columns[key_type_index];
            ColumnWithTypeAndName column_to_cast = { key_column_to_cast, key_type, "" };
            auto casted_column = castColumnAccurate(column_to_cast, float_64_type);
            key_column_to_cast = std::move(casted_column);
            key_type = float_64_type;
        }
    }


    ColumnPtr IPolygonDictionary::getColumn(
        const std::string& attribute_name,
        const DataTypePtr& result_type,
        const Columns& key_columns,
        const DataTypes&,
        const ColumnPtr& default_values_column) const
    {
        const auto requested_key_polygons = extractPolygons(key_columns);
        const auto& attribute = dict_struct.getAttribute(attribute_name, result_type);
        DefaultValueProvider default_value_provider(attribute.null_value, default_values_column);
        size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
        const auto& attribute_values_column = attributes_columns[attribute_index];
        auto result = attribute_values_column->cloneEmpty();
        result->reserve(requested_key_points.size());
        if (unlikely(attribute.is_nullable))
        {
            getItemsImpl<Field>(
                requested_key_polygons,
                [&](size_t row) { return (*attribute_values_column)[row]; },
                [&](Field& value) { result->insert(value); },
                default_value_provider);
        }
        else
        {
            auto type_call = [&](const auto& dictionary_attribute_type)
            {
                using Type = std::decay_t<decltype(dictionary_attribute_type)>;
                using AttributeType = typename Type::AttributeType;
                using ValueType = DictionaryValueType<AttributeType>;
                using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;
                using ColumnType = typename ColumnProvider::ColumnType;
                const auto attribute_values_column_typed = typeid_cast<const ColumnType*>(attribute_values_column.get());
                if (!attribute_values_column_typed)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "An attribute type should be same as dictionary type");
                ColumnType& result_column_typed = static_cast<ColumnType&>(*result);
                if constexpr (std::is_same_v<ValueType, Array>)
                {
                    getItemsImpl<ValueType>(
                        requested_key_polygons,
                        [&](size_t row) { return (*attribute_values_column)[row].get<Array>(); },
                        [&](Array& value) { result_column_typed.insert(value); },
                        default_value_provider);
                }
                else if constexpr (std::is_same_v<ValueType, StringRef>)
                {
                    getItemsImpl<ValueType>(
                        requested_key_points,
                        [&](size_t row) { return attribute_values_column->getDataAt(row); },
                        [&](StringRef value) { result_column_typed.insertData(value.data, value.size); },
                        default_value_provider);
                }
                else
                {
                    auto& attribute_data = attribute_values_column_typed->getData();
                    auto& result_data = result_column_typed.getData();
                    getItemsImpl<ValueType>(
                        requested_key_points,
                        [&](size_t row) { return attribute_data[row]; },
                        [&](auto value) { result_data.emplace_back(value); },
                        default_value_provider);
                }
            };
            callOnDictionaryAttributeType(attribute.underlying_type, type_call);
        }
        return result;
    }

    Pipe IPointDictionary::read(const Names& column_names, size_t, size_t) const
    {
        if (!configuration.store_point_key_column)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Set `store_point_key_column` setting in dictionary configuration to true to support reading from PointDictionary.");
        const auto& dictionary_structure_keys = *dict_struct.key;
        const auto& dictionary_key_attribute = dictionary_structure_keys[0];
        ColumnsWithTypeAndName result_columns;
        result_columns.reserve(column_names.size());
        for (const auto& column_name : column_names)
        {
            ColumnWithTypeAndName column_with_type;
            if (column_name == dictionary_key_attribute.name)
            {
                column_with_type.column = key_attribute_column;
                column_with_type.type = dictionary_key_attribute.type;
            }
            else
            {
                const auto& dictionary_attribute = dict_struct.getAttribute(column_name);
                size_t attribute_index = dict_struct.attribute_name_to_index.find(dictionary_attribute.name)->second;
                column_with_type.column = attributes_columns[attribute_index];
                column_with_type.type = dictionary_attribute.type;
            }
            column_with_type.name = column_name;
            result_columns.emplace_back(column_with_type);
        }
        auto source = std::make_shared<SourceFromSingleChunk>(Block(result_columns));
        return Pipe(std::move(source));
    }

    void IPointDictionary::setup()
    {
        const auto& dictionary_structure_keys = *dict_struct.key;

        key_attribute_column = dictionary_structure_keys[0].type->createColumn();
        attributes_columns.reserve(dict_struct.attributes.size());
        for (const auto& attribute : dict_struct.attributes)
        {
            auto column = attribute.type->createColumn();
            attributes_columns.emplace_back(std::move(column));
            if (attribute.hierarchical)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "{}: hierarchical attributes not supported for dictionary of point type",
                    getDictionaryID().getNameForLogs());
        }
    }

    void IPointDictionary::blockToAttributes(const DB::Block& block)
    {
        const auto rows = block.rows();
        size_t skip_key_column_offset = 1;
        for (size_t i = 0; i < attributes_columns.size(); ++i)
        {
            const auto& block_column = block.safeGetByPosition(i + skip_key_column_offset);
            const auto& column = block_column.column;
            attributes_columns[i]->assumeMutable()->insertRangeFrom(*column, 0, column->size());
        }

        points.reserve(points.size() + rows);
        point_index_to_attribute_value_index.reserve(point_index_to_attribute_value_index.size() + rows);
        const auto& key_column = block.safeGetByPosition(0).column;
        if (configuration.store_point_key_column)
            key_attribute_column->assumeMutable()->insertRangeFrom(*key_column, 0, key_column->size());
        extractPoints(key_column);
    }


    void IPointDictionary::loadData()
    {
        QueryPipeline pipeline(source_ptr->loadAll());

        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
            blockToAttributes(block);
    }

    void IPolygonDictionary::calculateBytesAllocated()
    {
        if (configuration.store_point_key_column)
            bytes_allocated += key_attribute_column->allocatedBytes();
        for (const auto& column : attributes_columns)
            bytes_allocated += column->allocatedBytes();
        bytes_allocated = points.size() * 2 * sizeof(Point);
    }

    std::vector<IPolygonDictionary::Point> IPolygonDictionary::extractPoints(const Columns& key_columns)
    {
        if (key_columns.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected two columns of coordinates with type Float64");
        const auto* column_x = typeid_cast<const ColumnVector<Float64>*>(key_columns[0].get());
        const auto* column_y = typeid_cast<const ColumnVector<Float64>*>(key_columns[1].get());
        if (!column_x || !column_y)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected columns of Float64");
        const auto rows = key_columns.front()->size();
        std::vector<Point> result;
        result.reserve(rows);
        for (size_t row = 0; row < rows; ++row)
        {
            auto x = column_x->getElement(row);
            auto y = column_y->getElement(row);
            if (isNaN(x) || isNaN(y))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "PolygonDictionary input point component must not be NaN");
            if (std::isinf(x) || std::isinf(y))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "PolygonDictionary input point component must not be infinite");
            result.emplace_back(x, y);
        }
        return result;
    }

    ColumnUInt8::Ptr IPointDictionary::hasKeys(const Columns& key_columns, const DataTypes&) const
    {
        std::vector<IPolygonDictionary::Polygon> polygons = extractPolygons(key_columns);
        auto result = ColumnUInt8::create(polygons.size());
        auto& out = result->getData();
        size_t keys_found = 0;
        for (size_t i = 0; i < polygons.size(); ++i)
        {
            size_t unused_find_result = 0;
            auto& polygon = polygons[i];
            out[i] = find(polygon, unused_find_result);
            keys_found += out[i];
        }
        query_count.fetch_add(polygons.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
        return result;
    }



    template <typename AttributeType, typename ValueGetter, typename ValueSetter, typename DefaultValueExtractor>
    void IPointDictionary::getItemsImpl(
        const std::vector<IPointDictionary::>& requested_key_polygons,
        ValueGetter&& get_value,
        ValueSetter&& set_value,
        DefaultValueExtractor& default_value_extractor) const
    {
        size_t point_index = 0;
        size_t keys_found = 0;

        for (size_t requested_key_index = 0; requested_key_index < requested_key_polygons.size(); ++requested_key_index)
        {
            const auto found = find(requested_key_polygons[requested_key_index], point_index);
            if (found)
            {
                size_t attribute_values_index = point_index_to_attribute_value_index[point_index];
                auto value = get_value(attribute_values_index);
                set_value(value);
                ++keys_found;
            }
            else
            {
                Field default_value = default_value_extractor.getDefaultValue(requested_key_index);
                if constexpr (std::is_same_v<AttributeType, Field>)
                {
                    set_value(default_value);
                }
                else if constexpr (std::is_same_v<AttributeType, Array>)
                {
                    set_value(default_value.get<Array>());
                }
                else if constexpr (std::is_same_v<AttributeType, StringRef>)
                {
                    auto default_value_string = default_value.get<String>();
                    set_value(default_value_string);
                }
                else
                {
                    set_value(default_value.get<NearestFieldType<AttributeType>>());
                }
            }
        }
        query_count.fetch_add(requested_key_polygons.size(), std::memory_order_relaxed);
        found_count.fetch_add(keys_found, std::memory_order_relaxed);
    }


    namespace
    {
        



        struct Data
        {
            std::vector<IPointDictionary::Point>& dest;
            std::vector<size_t>& ids;

            void NumofPoint(bool new_tuple_points = false )
            {
                ids.push_back((ids.empty() ? 0 : ids.back() + new_tuple_points));
            }
            void AddPoint(IPointDictionary::Coord x, IPointDictionary::Coord y) {
                auto& lastpoint = dest.back();
                laspoint.emplace_back(&x, &y);
            }
        };

        void addNewPoint(IPointDictionary::Coord x, IPointDictionary::Coord y, Data& data)
        {
            data.NumofPoint(true);
            data.addPoint(x, y);
        }
 

        void AddPointsbyArray(const ColumnPtr& column, Data& data)
        {
            const auto* ptr_points = typeid_cast<const ColumnArray*>(column.get());
            const auto* ptr_coord = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getData());

            if (!ptr_coord)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected coordinates to be of type Float64");
            const auto& offsets = ptr_points->getOffsets();
            IColumn::Offset prev_offset = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                if (offsets[i] - prev_offset != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "All points should be two-dimensional");
                prev_offset = offsets[i];
                addNewPoint(ptr_coord->getElement(2 * i), ptr_coord->getElement(2 * i + 1), data);
            }
        }



    void AddPointsbyTuple(const ColumnPtr& column, Data& data)
        {
            const auto* ptr_points = typeid_cast<const ColumnTuple*>(column.get());
            if (!ptr_points)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column of tuples representing points");
            if (ptr_points->tupleSize() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Points should be two-dimensional");
            const auto* column_x = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(0));
            const auto* column_y = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(1));
            if (!column_x || !column_y) throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected coordinates to be of type Float64");

            for (size_t i = 0; i < column_x->size(); ++i)
            {
                addNewPoint(column_x->getElement(i), column_y->getElement(i), data);
            }
        }
    }
    void IPointDictionary::extractPoints(const ColumnPtr& column)
    {
        Data data = { points, point_index_to_attribute_value_index };
        switch (configuration.input_type)
        {
        case InputType::Tuple:
            AddPointsbytuple(column, data);
            break;
        case InputType::Array:
            AddPointsbyArray(column, data);
            break;
        }
    }
}
