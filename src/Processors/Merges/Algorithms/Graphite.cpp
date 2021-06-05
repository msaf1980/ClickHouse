#include <Processors/Merges/Algorithms/Graphite.h>

namespace DB::Graphite
{

static const String rule_types_str[] = {"all", "plain", "tagged_regex"};

const String & RuleTypeStr(RuleType rule_type) {
    return rule_types_str[rule_type];
}

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

}
