#pragma once
#include "PointDictionary.h"
#include <vector>
namespace DB
{
    namespace bg = boost::geometry;
    using Coord = IPointDictionary::Coord;
    using Point = IPointDictionary::Point;
    using Polygon = IPointDictionary::Polygon;
    using Ring = IPointictionary::Ring;
    class PoitDictionarySimple : public IPointDictionary
    {
    public:
        PointDictionarySimple(
            const StorageID& dict_id_,
            const DictionaryStructure& dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            Configuration configuration_);
        std::shared_ptr<const IExternalLoadable> clone() const override;
    private:
        bool find(const Polygon& polygon, std::vector<size_t>& point_index) const override;
    }
}
