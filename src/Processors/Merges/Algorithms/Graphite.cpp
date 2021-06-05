#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/Graphite.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
 //   extern const int BAD_ARGUMENTS;
    //extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
    //extern const int UNKNOWN_STORAGE;
    //extern const int NO_REPLICA_NAME_GIVEN;
}

namespace DB::Graphite
{

static const Graphite::Pattern undef_pattern =
{ /// empty pattern for selectPatternForPath
        .regexp = nullptr,
        .regexp_str = "",
        .function = nullptr,
        .retentions = Graphite::Retentions(),
        .type = undef_pattern.TypeUndef,
};


const Graphite::RollupRule selectPatternForPath(
        const Graphite::Patterns & patterns,
        const StringRef path)
{
    const Graphite::Pattern * first_match = &undef_pattern;

    for (const auto & pattern : patterns)
    {
        if (!pattern.regexp)
        {
            /// Default pattern
            if (first_match->type == first_match->TypeUndef && pattern.type == pattern.TypeAll)
            {
                /// There is only default pattern for both retention and aggregation
                return std::pair(&pattern, &pattern);
            }
            if (pattern.type != first_match->type)
            {
                if (first_match->type == first_match->TypeRetention)
                {
                    return std::pair(first_match, &pattern);
                }
                if (first_match->type == first_match->TypeAggregation)
                {
                    return std::pair(&pattern, first_match);
                }
            }
        }
        else {
            if (pattern.regexp->match(path.data, path.size))
            {
                /// General pattern with matched path
                if (pattern.type == pattern.TypeAll)
                {
                    /// Only for not default patterns with both function and retention parameters
                    return std::pair(&pattern, &pattern);
                }
                if (first_match->type == first_match->TypeUndef)
                {
                    first_match = &pattern;
                    continue;
                }
                if (pattern.type != first_match->type)
                {
                    if (first_match->type == first_match->TypeRetention)
                    {
                        return std::pair(first_match, &pattern);
                    }
                    if (first_match->type == first_match->TypeAggregation)
                    {
                        return std::pair(&pattern, first_match);
                    }
                }
            }
        }
    }

    return {nullptr, nullptr};
}

/** Is used to order Graphite::Retentions by age and precision descending.
  * Throws exception if not both age and precision are less or greater then another.
  */
static bool compareRetentions(const Retention & a, const Retention & b)
{
    if (a.age > b.age && a.precision > b.precision)
    {
        return true;
    }
    else if (a.age < b.age && a.precision < b.precision)
    {
        return false;
    }
    String error_msg = "age and precision should only grow up: "
        + std::to_string(a.age) + ":" + std::to_string(a.precision) + " vs "
        + std::to_string(b.age) + ":" + std::to_string(b.precision);
    throw Exception(
        error_msg,
        DB::ErrorCodes::BAD_ARGUMENTS);
}

/** Read the settings for Graphite rollup from config.
  * Example
  *
  * <graphite_rollup>
  *     <path_column_name>Path</path_column_name>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>3600</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>60</precision>
  *         </retention>
  *     </pattern>
  *     <default>
  *         <function>max</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>60</precision>
  *         </retention>
  *         <retention>
  *             <age>3600</age>
  *             <precision>300</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>3600</precision>
  *         </retention>
  *     </default>
  * </graphite_rollup>
  */
static void
appendGraphitePattern(const Poco::Util::AbstractConfiguration & config, const String & config_element, Patterns & patterns)
{
    Pattern pattern;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (key == "regexp")
        {
            pattern.regexp_str = config.getString(config_element + ".regexp");
            pattern.regexp = std::make_shared<OptimizedRegularExpression>(pattern.regexp_str);
        }
        else if (key == "function")
        {
            String aggregate_function_name_with_params = config.getString(config_element + ".function");
            String aggregate_function_name;
            Array params_row;
            getAggregateFunctionNameAndParametersArray(
                aggregate_function_name_with_params, aggregate_function_name, params_row, "GraphiteMergeTree storage initialization");

            /// TODO Not only Float64
            AggregateFunctionProperties properties;
            pattern.function = AggregateFunctionFactory::instance().get(
                aggregate_function_name, {std::make_shared<DataTypeFloat64>()}, params_row, properties);
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.emplace_back(Graphite::Retention{
                .age = config.getUInt(config_element + "." + key + ".age"),
                .precision = config.getUInt(config_element + "." + key + ".precision")});
        }
        else
            throw Exception("Unknown element in config: " + key, DB::ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (!pattern.function && pattern.retentions.empty())
        throw Exception(
            "At least one of an aggregate function or retention rules is mandatory for rollup patterns in GraphiteMergeTree",
            DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (!pattern.function)
    {
        pattern.type = pattern.TypeRetention;
    }
    else if (pattern.retentions.empty())
    {
        pattern.type = pattern.TypeAggregation;
    }
    else
    {
        pattern.type = pattern.TypeAll;
    }

    if (pattern.type & pattern.TypeAggregation) /// TypeAggregation or TypeAll
        if (pattern.function->allocatesMemoryInArena())
            throw Exception(
                "Aggregate function " + pattern.function->getName() + " isn't supported in GraphiteMergeTree", DB::ErrorCodes::NOT_IMPLEMENTED);

    /// retention should be in descending order of age.
    if (pattern.type & pattern.TypeRetention) /// TypeRetention or TypeAll
        std::sort(pattern.retentions.begin(), pattern.retentions.end(), compareRetentions);

    patterns.emplace_back(pattern);
}

void setGraphitePatternsFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_element, Graphite::Params & params)
{
    if (!config.has(config_element))
        throw Exception("No '" + config_element + "' element in configuration file", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    params.config_name = config_element;
    params.path_column_name = config.getString(config_element + ".path_column_name", "Path");
    params.time_column_name = config.getString(config_element + ".time_column_name", "Time");
    params.value_column_name = config.getString(config_element + ".value_column_name", "Value");
    params.version_column_name = config.getString(config_element + ".version_column_name", "Timestamp");

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "pattern"))
        {
            appendGraphitePattern(config, config_element + "." + key, params.patterns);
        }
        else if (key == "default")
        {
            /// See below.
        }
        else if (key == "path_column_name" || key == "time_column_name" || key == "value_column_name" || key == "version_column_name")
        {
            /// See above.
        }
        else
            throw Exception("Unknown element in config: " + key, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (config.has(config_element + ".default"))
        appendGraphitePattern(config, config_element + "." + ".default", params.patterns);
}

}
