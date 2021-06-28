#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/Graphite.h>

#include <string_view>
#include <vector>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

using namespace std::literals;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
 }

namespace DB::Graphite
{
static const String rule_types_str[] = {"all", "plain", "tagged", "tagged_map"};

const String & ruleTypeStr(RuleType rule_type)
{
    return rule_types_str[rule_type];
}

RuleType ruleType(const String & s)
{
    if (s == "all")
        return RuleTypeAll;
    else if (s == "plain")
        return RuleTypePlain;
    else if (s == "tagged")
        return RuleTypeTagged;
    else if (s == "tagged_map")
        return RuleTypeTaggedMap;
    else
        throw Exception("invalid rule type: " + s, DB::ErrorCodes::BAD_ARGUMENTS);
}

static const Graphite::Pattern undef_pattern =
{ /// empty pattern for selectPatternForPath
        .rule_type = RuleTypeAll,
        .regexp = nullptr,
        .tagged_map = { },
        .regexp_str = "",
        .function = nullptr,
        .retentions = Graphite::Retentions(),
        .type = undef_pattern.TypeUndef,
};

struct Tag {
    std::string_view key;
    std::string_view value;
};

bool splitTags(std::string_view path, size_t tagged_pos, std::vector<Tag> & tags)
{
    std::string_view s{path.substr(tagged_pos+1)};
    tags.push_back({"name"sv, path.substr(0, tagged_pos)});

    size_t last = 0;
    size_t key_pos;
    std::string_view key;
    do {
        if ((key_pos = s.find('=', last)) == s.npos) /* tags are broken? */
            return false;

        key = s.substr(last, key_pos - last);
        key_pos++;
        if ((last = s.find('&', key_pos)) == s.npos)
        {
            tags.push_back({key, s.substr(key_pos)});
            break;
        }
        else
        {
            tags.push_back({key, s.substr(key_pos, last - key_pos)});
        }
        last++;
    } while (1);

    return true;
}

static const Patterns & selectPatternsForMetricType(const Graphite::Params & params, std::string_view path, std::vector<Tag> & tags)
{
    if (params.patterns_typed)
    {
        size_t tagged_pos = path.find('?');
        if (tagged_pos == path.npos)
            return params.patterns_plain;

        if (params.patterns_tagged_map)
        {
            tags.reserve(64);
            splitTags(path, tagged_pos, tags);
            return params.patterns_tagged;
        }
    }

    return params.patterns;
}

bool checkTaggedMap(
    const std::vector<Tag> & tags,
    const Graphite::Pattern & pattern)
{
    if (tags.size() < pattern.tagged_map.size())
        return false;

    ssize_t cmp_count = pattern.tagged_map.size();
    for (const auto & tag : tags)
    {
        auto cmp_tag = pattern.tagged_map.find(tag.key);
        if (cmp_tag != pattern.tagged_map.end())
        {
            switch (cmp_tag->second.op)
            {
                case TaggedTermEq:
                    if (tag.value != cmp_tag->second.value)
                        return false;
                    cmp_count--;
                    break;
                case TaggedTermNe:
                    if (tag.value == cmp_tag->second.value)
                        return false;
                    cmp_count--;
                    break;
                case TaggedTermMatch:
                    if (!cmp_tag->second.regexp->match(tag.value.data(), tag.value.size()))
                        return false;
                    cmp_count--;
                    break;
                case TaggedTermNotMatch:
                    if (cmp_tag->second.regexp->match(tag.value.data(), tag.value.size()))
                        return false;
                    cmp_count--;
                    break;
            }
        }
    }
    return cmp_count == 0;
}

Graphite::RollupRule selectPatternForPath(
        const Graphite::Params & params,
        const StringRef path)
{
    const Graphite::Pattern * first_match = &undef_pattern;

    std::vector<Tag> tags;
    std::string_view path_view = path.toView();
    const Patterns & patterns_check = selectPatternsForMetricType(params, path_view, tags);

    for (const auto & pattern : patterns_check)
    {
        if (pattern.regexp_str.empty())
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
        else
        {
            bool found;
            if (pattern.rule_type == RuleTypeTaggedMap)
                found = checkTaggedMap(tags, pattern);
            else
                found = pattern.regexp->match(path.data, path.size);

            if (found)
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

bool operator==(const Retention & a, const Retention & b)
{
    return a.age == b.age && a.precision == b.precision;
}

std::ostream & operator<<(std::ostream & stream, const Retentions & a)
{
    stream << "{ ";
    for (size_t i = 0; i < a.size(); i++)
    {
        if (i > 0)
            stream << ",";
        stream << " { age = " << a[i].age << ", precision = " << a[i].precision << " }";
    }
    stream << " }";

    return stream;
}

bool operator==(const Pattern & a, const Pattern & b)
{
    // equal
    // Retentions retentions;    /// Must be ordered by 'age' descending.
    if (a.type != b.type || a.regexp_str != b.regexp_str || a.rule_type != b.rule_type)
        return false;

    if (a.function == nullptr)
    {
        if (b.function != nullptr)
            return false;
    }
    else if (b.function == nullptr)
    {
        return false;
    }
    else if (a.function->getName() != b.function->getName())
    {
        return false;
    }

    return a.retentions == b.retentions;
}

std::ostream & operator<<(std::ostream & stream, const Pattern & a)
{
    stream << "{ rule_type = " << ruleTypeStr(a.rule_type);
    if (!a.regexp_str.empty())
        stream << ", regexp = '" << a.regexp_str << "'";
    if (a.function != nullptr)
        stream << ", function = " << a.function->getName();
    if (!a.retentions.empty())
    {
        stream << ",\n  retentions = {\n";
        for (size_t i = 0; i < a.retentions.size(); i++)
        {
            stream << "    { " << a.retentions[i].age << ", " << a.retentions[i].precision << " }";
            if (i < a.retentions.size() - 1)
                stream << ",";
            stream << "\n";
        }
        stream << "  }\n";
    }
    else
        stream << " ";

    stream << "}";
    return stream;
}

std::string buildTaggedRegex(std::string regexp_str)
{
    boost::erase_all(regexp_str, " "); /* cleanup spaces */
    size_t name_pos = regexp_str.find(';');
    if (name_pos != regexp_str.npos)
    {
        /* tag in format
         *
         * tag1=value1 ; tag2=VALUE2_REGEX ; tag3=value3
         * or
         * name ; tag1=value1 ; tag2=VALUE2_REGEX ; tag3=value3
         * or for one tag add ';' to the end
         * tag1=value1 ;
         */

        std::vector<std::string> tags;

        boost::split(tags, regexp_str, boost::is_any_of(";"));
        /* remove empthy elements */
        using namespace std::string_literals;
        tags.erase(std::remove(tags.begin(), tags.end(), ""s), tags.end());
        if (tags[0].find('=') == tags[0].npos)
        {
            if (tags.size() == 1) /* only name */
                return tags[0] + "\\?";
            /* start with name value */
            regexp_str = tags[0] + "\\?(.*&)?";
            tags.erase(std::begin(tags));
        }
        else
            regexp_str = "[\\?&]";

        std::sort(std::begin(tags), std::end(tags)); /* sorted tag keys */
        regexp_str += boost::algorithm::join(tags, "&(.*&)?");
        regexp_str += "(&.*)?$"; /* close regex */
        return regexp_str;
    }
    return regexp_str;
}

void parseTaggedMap(struct Pattern & pattern, const std::string s)
{
    std::vector<std::string> tags_vec;
    split(tags_vec, s, boost::is_any_of(";"));
    for (const auto & v : tags_vec) {
        TaggedNode tagged_node;
        std::string key;
        size_t pos = v.find_first_of("!=~", 0);
        if (pos == v.npos)
        {
            key = "name";
            tagged_node.value = v;
            tagged_node.op = TaggedTermEq;
        }
        else
        {
            std::string op;
            op.reserve(3);
            key = v.substr(0, pos);
            if (key == "__name__")
                key = "name";
            for (size_t i = pos; i < pos + 3; i++) {
                if (v[i] == '=' || v[i] == '~' || v[i] == '!')
                    op += v[i];
                else {
                    pos = i;
                    break;
                }
            }
            if (op == "=")
                tagged_node.op = TaggedTermEq;
            else if (op == "=~")
                tagged_node.op = TaggedTermMatch;
            else if (op == "!=")
                tagged_node.op = TaggedTermNe;
            else if (op == "!=~")
                tagged_node.op = TaggedTermNotMatch;
            else
                throw Exception("Unknown comparator in tagged map: " + op, DB::ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

            tagged_node.value = v.substr(pos);
            if (tagged_node.op == TaggedTermMatch || tagged_node.op == TaggedTermNotMatch)
                tagged_node.regexp = std::make_shared<OptimizedRegularExpression>(tagged_node.value);
        }
        pattern.tagged_map[key] = tagged_node;
    }
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
static RuleType
appendGraphitePattern(const Poco::Util::AbstractConfiguration & config, const String & config_element, Patterns & patterns, bool default_rule)
{
    Pattern pattern;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (key == "regexp")
        {
            pattern.regexp_str = config.getString(config_element + ".regexp");
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
        else if (key == "rule_type")
        {
            String rule_type = config.getString(config_element + ".rule_type");
            pattern.rule_type = ruleType(rule_type);
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

    if (!pattern.regexp_str.empty())
    {
        if (pattern.rule_type == RuleTypeTaggedMap)
        {
            boost::erase_all(pattern.regexp_str, " ");
            parseTaggedMap(pattern, pattern.regexp_str);
        }
        else
        {
            if (pattern.rule_type == RuleTypeTagged)
            {
                pattern.regexp_str = buildTaggedRegex(pattern.regexp_str);
            }
            pattern.regexp = std::make_shared<OptimizedRegularExpression>(pattern.regexp_str);
        }
    }

    if (!pattern.function && pattern.retentions.empty())
        throw Exception(
            "At least one of an aggregate function or retention rules is mandatory for rollup patterns in GraphiteMergeTree",
            DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (default_rule && pattern.rule_type != RuleTypeAll)
    {
        throw Exception(
            "Default must have rule_type all for rollup patterns in GraphiteMergeTree",
            DB::ErrorCodes::BAD_ARGUMENTS);
    }

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
    return pattern.rule_type;
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

    params.patterns_typed = false;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "pattern"))
        {
            auto pattern_type = appendGraphitePattern(config, config_element + "." + key, params.patterns, false);
            if (pattern_type != RuleTypeAll)
                params.patterns_typed = true;
            if (pattern_type == RuleTypeTaggedMap)
                params.patterns_tagged_map = true;
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
        appendGraphitePattern(config, config_element + "." + ".default", params.patterns, true);

    for (const auto & pattern : params.patterns)
    {
        if (pattern.rule_type == RuleTypeAll)
        {
            if (params.patterns_typed)
            {
                params.patterns_plain.push_back(pattern);
                params.patterns_tagged.push_back(pattern);
            }
        }
        else if (pattern.rule_type == RuleTypePlain)
        {
            params.patterns_plain.push_back(pattern);
        }
        else if (pattern.rule_type == RuleTypeTagged || pattern.rule_type == RuleTypeTaggedMap)
        {
            params.patterns_tagged.push_back(pattern);
        }
        else
        {
            throw Exception("Unhandled rule_type in config: " + ruleTypeStr(pattern.rule_type), ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        }
    }
}

}
