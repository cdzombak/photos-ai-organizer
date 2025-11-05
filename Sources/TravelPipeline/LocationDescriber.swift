import Core

struct LocationDescriber {
    func describe(cluster: TravelCluster, place: PlaceInfo, baselinePlace: PlaceInfo?) -> String {
        if cluster.isCountryAggregate {
            return place.countryName ?? place.description
        }
        let city = place.cityName
        let region = place.regionName
        let country = place.countryName ?? place.description
        let baselineCountry = cluster.baselineCountryCode?.uppercased() ?? baselinePlace?.countryCode?.uppercased()
        let clusterCountry = (cluster.countryCode ?? place.countryCode)?.uppercased()
        if let baselineCountry, let clusterCountry, baselineCountry == clusterCountry {
            if let baselineRegion = baselinePlace?.regionName,
               let region = region,
               baselineRegion.caseInsensitiveCompare(region) == .orderedSame {
                return city ?? region
            }
            if let city = city, let region = region {
                return "\(city), \(region)"
            }
            return city ?? region ?? country
        } else {
            if let city = city {
                if let country = place.countryName {
                    return "\(city), \(country)"
                }
                return city
            }
            return country
        }
    }
}
