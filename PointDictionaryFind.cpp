#include "PointDictionaryImplementations.h"
#include "DictionaryFactory.h"
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <numeric>
namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
    }
    PointDictionarySimple::PointDictionarySimple(
        const StorageID& dict_id_,
        const DictionaryStructure& dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        Configuration configuration_)
        : IPointDictionary(dict_id_, dict_struct_, std::move(source_ptr_), dict_lifetime_, configuration_)
    {
    }
    std::shared_ptr<const IExternalLoadable> PointDictionarySimple::clone() const
    {
        return std::make_shared<PolygonDictionarySimple>(
            this->getDictionaryID(),
            this->dict_struct,
            this->source_ptr->clone(),
            this->dict_lifetime,
            this->configuration);
    }
    bool PointDictionarySimple::find(const Polygon& polygon, std::vector<size_t>& point_index) const
    {
        bool found = false;
        for (size_t i = 0; i < points.size(); ++i)
        {
            if (bg::covered_by(points[i], polygon))
            {
                point_index.emplace_back(i);
                found = true;
                
            }
        }
        return found;
    }
   
    template <class PointDictionary>
    DictionaryPtr createLayout(const std::string&,
        const DictionaryStructure& dict_struct,
        const Poco::Util::AbstractConfiguration& config,
        const std::string& config_prefix,
        DictionarySourcePtr source_ptr,
        ContextPtr /* global_context */,
        bool /*created_from_ddl*/)
    {
        const String database = config.getString(config_prefix + ".database", "");
        const String name = config.getString(config_prefix + ".name");
        if (!dict_struct.key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'key' is required for a point dictionary");
        if (dict_struct.key->size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The 'key' should consist of a single attribute for a point dictionary");
        const auto key_type = (*dict_struct.key)[0].type;
        const auto f64 = std::make_shared<DataTypeFloat64>();
        const auto tuple_float = std::make_shared<DataTypeTuple>(std::vector<DataTypePtr>{f64, f64}));
        const auto array_float = std::make_shared<DataTypeArray>(f64);
        IPointDictionary::InputType input_type;
        if (key_type->equals(tuple_float))
        {
            input_type = IPointDictionary::InputType::Tuple;
            
        }
        else if (key_type->equals(array_float))
        {
            input_type = IPointDictionary::InputType::Array;
            
        }
       
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The key type {} is not one of the following allowed types for a point dictionary: {} {}",
                key_type->getName(),
                tuple_float.getName(),
                array_float.getName(),
               
        }
        const auto& layout_prefix = config_prefix + ".layout";
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(layout_prefix, keys);
        const auto& dict_prefix = layout_prefix + "." + keys.front();
        IPointDictionary::Configuration configuration
        {
            .input_type = input_type,
            .store_point_key_column = config.getBool(dict_prefix + ".store_point_key_column", false)
        };
        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements range_min and range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                name);
        const DictionaryLifetime dict_lifetime{ config, config_prefix + ".lifetime" };
        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
       
            return std::make_unique<PointDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, configuration);
    }
    void registerDictionaryPolygon(DictionaryFactory& factory)
    {
        factory.registerLayout("point_simple", createLayout<PointDictionarySimple>, true);
        
    }
}
