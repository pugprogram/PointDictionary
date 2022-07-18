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

    void IPointDictionary::setup()
    {
        const auto& dictionary_structure_keys = *dict_struct.key;

        key_attribute_column = dictionary_structure_keys[0].type->createColumn();

        attributes_columns.reserve(dict_struct.attributes.size());

        for (const auto& attribute : dict_struct.attributes)
        {
            auto column = attribute.type->createColumn();
            attributes_columns.emplace_back(std::move(column));
        }
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

    if (configuration.store_point_key_column) key_attribute_column->assumeMutable()->insertRangeFrom(*key_column, 0, key_column->size());
    extractPoints(key_column);
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


ColumnUInt8::Ptr IPointDictionary::hasKeys(const Columns& key_columns, const DataTypes&) const
{
    std::vector<IPointDictionary::Polygon> polygons = extractPoLygons(key_columns);

    auto result = ColumnUInt8::create(polygons.size());

    auto& out = result->getData();
    size_t keys_found = 0;

    for (size_t i = 0; i < polygons.size(); ++i)
    {
        size_t unused_find_result=0;

        auto& polygon = polygons[i];
        out[i] = find(polygon, unused_find_result);
        keys_found += out[i];

    }

    query_count.fetch_add(polygons.size(), std::memory_order_relaxed);

    found_count.fetch_add(keys_found, std::memory_order_relaxed);
    return result;
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
    polint_index_to_attribute_value_index.reserve(point_index_to_attribute_value_index.size() + rows);
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



void IPointDictionary::calculateBytesAllocated()
{
    if (configuration.store_point_key_column)
        bytes_allocated += key_attribute_column->allocatedBytes();
    for (const auto& column : attributes_columns)
        bytes_allocated += column->allocatedBytes();
    for (auto& point : points)
        bytes_allocated += 2 * sizeof(Points);
}

}
