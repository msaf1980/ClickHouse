#pragma once

#include <unordered_map>

#include <Poco/Util/AbstractConfiguration.h>

#include <common/StringRef.h>
#include <Common/OptimizedRegularExpression.h>

namespace DB
{

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

}

/** Intended for implementation of "rollup" - aggregation (rounding) of older data
  *  for a table with Graphite data (Graphite is the system for time series monitoring).
  *
  * Table with graphite data has at least the following columns (accurate to the name):
  * Path, Time, Value, Version
  *
  * Path - name of metric (sensor);
  * Time - time of measurement;
  * Value - value of measurement;
  * Version - a number, that for equal pairs of Path and Time, need to leave only record with maximum version.
  *
  * Each row in a table correspond to one value of one sensor.
  *
  * Pattern should contain function, retention scheme, or both of them. The order of patterns does mean as well:
  *   * Aggregation OR retention patterns should be first
  *   * Then aggregation AND retention full patterns have to be placed
  *   * default pattern without regexp must be the last
  *
  * Rollup rules are specified in the following way:
  *
  * pattern
  *     regexp
  *     function
  * pattern
  *     regexp
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     regexp
  *     function
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     ...
  * default
  *     function
  *        age -> precision
  *     ...
  *
  * regexp - pattern for sensor name
  * default - if no pattern has matched
  *
  * age - minimal data age (in seconds), to start rounding with specified precision.
  * precision - rounding precision (in seconds)
  *
  * function - name of aggregate function to be applied for values, that time was rounded to same.
  *
  * Example:
  *
  * <graphite_rollup>
  *     <pattern>
  *         <regexp>\.max$</regexp>
  *         <function>max</function>
  *     </pattern>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>5</precision>
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
namespace DB::Graphite
{

// sync with rule_types_str
enum RuleType { RuleTypeAll = 0, RuleTypePlain = 1, RuleTypeTagged = 2, RuleTypeTaggedMap = 3 };

const String & ruleTypeStr(RuleType rule_type);

struct Retention
{
    UInt32 age;
    UInt32 precision;
};

bool operator==(const Retention & a, const Retention & b);

using Retentions = std::vector<Retention>;

std::ostream &operator<<(std::ostream & stream, const Retentions & a);

enum TaggedTerm { TaggedTermEq = 0, TaggedTermMatch = 1, TaggedTermNe = 2, TaggedTermNotMatch = 3 };

struct TaggedNode {
    TaggedTerm op;
    std::string value;
    std::shared_ptr<OptimizedRegularExpression> regexp;
};

struct equal : public std::equal_to<>
{
    using is_transparent = void;
};

struct string_hash {
    using is_transparent = void;
    using key_equal = std::equal_to<>;  // Pred to use
    using hash_type = std::hash<std::string_view>;  // just a helper local type
    size_t operator()(std::string_view txt) const { return hash_type{}(txt); }
    size_t operator()(const std::string& txt) const { return hash_type{}(txt); }
    size_t operator()(const char* txt) const { return hash_type{}(txt); }
};

struct Pattern
{
    RuleType rule_type = RuleTypeAll;
    std::shared_ptr<OptimizedRegularExpression> regexp;
    std::unordered_map<std::string, TaggedNode, string_hash, equal> tagged_map;
    std::string regexp_str;
    AggregateFunctionPtr function;
    Retentions retentions;    /// Must be ordered by 'age' descending.
    enum { TypeUndef, TypeRetention, TypeAggregation, TypeAll } type = TypeAll; /// The type of defined pattern, filled automatically
};

bool operator==(const Pattern & a, const Pattern & b);
std::ostream &operator<<(std::ostream & stream, const Pattern & a);

using Patterns = std::vector<Pattern>;
using RetentionPattern = Pattern;
using AggregationPattern = Pattern;

struct Params
{
    String config_name;
    String path_column_name;
    String time_column_name;
    String value_column_name;
    String version_column_name;
    bool patterns_typed;
    bool patterns_tagged_map;
    Graphite::Patterns patterns;
    Graphite::Patterns patterns_plain;
    Graphite::Patterns patterns_tagged;
};

using RollupRule = std::pair<const RetentionPattern *, const AggregationPattern *>;

Graphite::RollupRule selectPatternForPath(const Graphite::Params & params, const StringRef path);

void setGraphitePatternsFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_element, Graphite::Params & params);

}
