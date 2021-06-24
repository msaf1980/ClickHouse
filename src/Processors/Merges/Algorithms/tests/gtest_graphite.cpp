#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <Common/Config/ConfigProcessor.h>

using namespace DB;

static int regAggregateFunctions = 0;

void tryRegisterAggregateFunctions()
{
    if (!regAggregateFunctions)
    {
        registerAggregateFunctions();
        regAggregateFunctions = 1;
    }
}

static ConfigProcessor::LoadedConfig loadConfiguration(const std::string & config_path)
{
    ConfigProcessor config_processor(config_path, true, true);
    ConfigProcessor::LoadedConfig config = config_processor.loadConfig(false);
    return config;
}

static ConfigProcessor::LoadedConfig loadConfigurationFromStream(std::istringstream & xml_istream)
{
    char tmp_file[19];
    strcpy(tmp_file, "/tmp/rollup-XXXXXX");
    int fd = mkstemp(tmp_file);
    if (fd == -1)
    {
        throw std::runtime_error(strerror(errno));
    }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    for (std::string line; std::getline(xml_istream, line);)
    {
        write(fd, line.c_str(), line.size());
        write(fd, "\n", 1);
    }
    close(fd);
    auto config_path = std::string(tmp_file) + ".xml";
    if (std::rename(tmp_file, config_path.c_str()))
    {
        int err = errno;
        remove(tmp_file);
        throw std::runtime_error(strerror(err));
    }
    ConfigProcessor::LoadedConfig config = loadConfiguration(config_path);
    remove(tmp_file);
#pragma GCC diagnostic pop
    return config;
}

static Graphite::Params setGraphitePatternsFromStream(std::istringstream & xml_istream)
{
    auto config = loadConfigurationFromStream(xml_istream);

    Graphite::Params params;
    setGraphitePatternsFromConfig(*config.configuration.get(), "graphite_rollup", params);

    return params;
}

typedef struct
{
    Graphite::RuleType rule_type;
    std::string regexp_str;
    String function;
    Graphite::Retentions retentions;
} pattern_for_check;


bool checkRule(const Graphite::Pattern & pattern, const pattern_for_check & pattern_check,
    const std::string & typ, const std::string & path, std::string & message)
{
    bool rule_type_eq = (pattern.rule_type == pattern_check.rule_type);
    bool regexp_eq = (pattern.regexp_str == pattern_check.regexp_str);
    bool function_eq = (pattern.function == nullptr && pattern_check.function == "")
                    || (pattern.function->getName() == pattern_check.function);
    bool retentions_eq = (pattern.retentions == pattern_check.retentions);

    if (rule_type_eq && regexp_eq && function_eq && retentions_eq)
        return true;

    message = typ + " rollup rule mismatch ( " +
        (rule_type_eq ? "" : "rule_type ") + (regexp_eq ? "" : "regexp ") +
        (function_eq ? "" : "function ") + (retentions_eq ? "" : "retentions ") + ") for '" + path + "'";
    return false;
}

std::ostream & operator<<(std::ostream & stream, const pattern_for_check & a)
{
    stream << "{ rule_type = " << ruleTypeStr(a.rule_type);
    if (a.regexp_str.size() > 0)
        stream << ", regexp = '" << a.regexp_str << "'";
    if (a.function.size() != 0)
        stream << ", function = " << a.function;
    if (a.retentions.size() > 0)
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

typedef struct
{
    std::string path;
    pattern_for_check retention_want;
    pattern_for_check aggregation_want;
} patterns_for_path;

TEST(GraphiteTest, testSelectPattern)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::istringstream // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_istream(R"END(<yandex>
<graphite_rollup>
    <pattern>
 	    <regexp>\.sum$</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
		<regexp>^((.*)|.)sum\?</regexp>
		<function>sum</function>
	</pattern>
	<pattern>
 		<regexp>\.max$</regexp>
 		<function>max</function>
	</pattern>
	<pattern>
		<regexp>^((.*)|.)max\?</regexp>
		<function>max</function>
	</pattern>
	<pattern>
 		<regexp>\.min$</regexp>
 		<function>min</function>
	</pattern>
	<pattern>
 		<regexp>^((.*)|.)min\?</regexp>
 		<function>min</function>
	</pattern>
	<pattern>
 		<regexp>\.(count|sum|sum_sq)$</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
 		<regexp>^((.*)|.)(count|sum|sum_sq)\?</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
 		<regexp>^retention\.</regexp>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
	</pattern>
 	<default>
 		<function>avg</function>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>300</precision>
 	    </retention>
 	    <retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
 	</default>
</graphite_rollup>
</yandex>
)END");

    Graphite::Params params = setGraphitePatternsFromStream(xml_istream);

    // Retentions must be ordered by 'age' descending.
    std::vector<patterns_for_path> tests
    {
        {
            "test.sum",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.sum$)END", "sum", { } }
        },
        {
            "__name__=sum?env=test&tag=Fake3",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)sum\?)END", "sum", { } }
        },
        {
            "test.max",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.max$)END", "max", { } },
        },
        {
            "__name__=max?env=test&tag=Fake4",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)max\?)END", "max", { } },
        },
        {
            "test.min",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(\.min$)END", "min", { } },
        },
        {
            "__name__=min?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)min\?)END", "min", { } },
        },
        {
            "retention.count",
            { Graphite::RuleTypeAll, R"END(^retention\.)END", "", { { 86400, 3600 }, { 0, 60 } } }, // ^retention
            { Graphite::RuleTypeAll, R"END(\.(count|sum|sum_sq)$)END", "sum", { } },
        },
        {
            "__name__=retention.count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "__name__=count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "test.p95",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "__name__=p95?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "default",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "__name__=default?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        }
    };
    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        std:: string message;
        if (!checkRule(*rule.first, t.retention_want, "retention", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.first << "\n, want\n" << t.retention_want << "\n";
        if (!checkRule(*rule.second, t.aggregation_want, "aggregation", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.second << "\n, want\n" << t.aggregation_want << "\n";
    }
}

TEST(GraphiteTest, testSelectPatternTyped)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::istringstream // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_istream(R"END(<yandex>
<graphite_rollup>
	<pattern>
        <rule_type>plain</rule_type>
 		<regexp>\.sum$</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
        <rule_type>tagged</rule_type>
		<regexp>^((.*)|.)sum\?</regexp>
		<function>sum</function>
	</pattern>
	<pattern>
        <rule_type>plain</rule_type>
 		<regexp>\.max$</regexp>
 		<function>max</function>
	</pattern>
	<pattern>
        <rule_type>tagged</rule_type>
		<regexp>^((.*)|.)max\?</regexp>
		<function>max</function>
	</pattern>
	<pattern>
        <rule_type>plain</rule_type>
 		<regexp>\.min$</regexp>
 		<function>min</function>
	</pattern>
	<pattern>
		<rule_type>tagged</rule_type>
 		<regexp>^((.*)|.)min\?</regexp>
 		<function>min</function>
	</pattern>
	<pattern>
		<rule_type>plain</rule_type>
 		<regexp>\.(count|sum|sum_sq)$</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
		<rule_type>tagged</rule_type>
 		<regexp>^((.*)|.)(count|sum|sum_sq)\?</regexp>
 		<function>sum</function>
	</pattern>
	<pattern>
		<rule_type>plain</rule_type>
 		<regexp>^retention\.</regexp>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
	</pattern>
    <pattern>
		<rule_type>tagged</rule_type>
 		<regexp><![CDATA[[\?&]retention=hour(&?.*)$]]></regexp>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
	</pattern>
 	<default>
 		<function>avg</function>
 		<retention>
 			<age>0</age>
 			<precision>60</precision>
 		</retention>
 		<retention>
 			<age>3600</age>
 			<precision>300</precision>
 		</retention>
 		<retention>
 			<age>86400</age>
 			<precision>3600</precision>
 		</retention>
 	</default>
</graphite_rollup>
</yandex>
)END");

    Graphite::Params params = setGraphitePatternsFromStream(xml_istream);

    // Retentions must be ordered by 'age' descending.
    std::vector<patterns_for_path> tests
    {
        {
            "test.sum",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.sum$)END", "sum", { } }
        },
        {
            "__name__=sum?env=test&tag=Fake3",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)sum\?)END", "sum", { } }
        },
        {
            "test.max",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.max$)END", "max", { } },
        },
        {
            "__name__=max?env=test&tag=Fake4",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)max\?)END", "max", { } },
        },
        {
            "test.min",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypePlain, R"END(\.min$)END", "min", { } },
        },
        {
            "__name__=min?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)min\?)END", "min", { } },
        },
        {
            "retention.count",
            { Graphite::RuleTypePlain, R"END(^retention\.)END", "", { { 86400, 3600 }, { 0, 60 } } }, // ^retention
            { Graphite::RuleTypePlain, R"END(\.(count|sum|sum_sq)$)END", "sum", { } },
        },
        {
            "__name__=count?env=test&retention=hour&tag=Fake5",
            { Graphite::RuleTypeTagged, R"END([\?&]retention=hour(&?.*)$)END", "", { { 86400, 3600 }, { 0, 60 } } }, // tagged retention=hour
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "__name__=count?env=test&retention=hour",
            { Graphite::RuleTypeTagged, R"END([\?&]retention=hour(&?.*)$)END", "", { { 86400, 3600 }, { 0, 60 } } }, // tagged retention=hour
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "__name__=count?env=test&tag=Fake5",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeTagged, R"END(^((.*)|.)(count|sum|sum_sq)\?)END", "sum", { } },
        },
        {
            "test.p95",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "__name__=p95?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "default",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        },
        {
            "__name__=default?env=test&tag=FakeNo",
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
            { Graphite::RuleTypeAll, "", "avg", { { 86400, 3600 }, { 3600, 300 }, { 0, 60 } } }, //default
        }
    };
    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        std:: string message;
        if (!checkRule(*rule.first, t.retention_want, "retention", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.first << "\n, want\n" << t.retention_want << "\n";
        if (!checkRule(*rule.second, t.aggregation_want, "aggregation", t.path, message))
            ADD_FAILURE() << message << ", got\n" << *rule.second << "\n, want\n" << t.aggregation_want << "\n";
    }
}
