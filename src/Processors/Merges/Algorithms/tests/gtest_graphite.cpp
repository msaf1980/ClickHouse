#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace fs = std::filesystem;

#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

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

const auto dir = fs::path(__FILE__).parent_path();

static ConfigProcessor::LoadedConfig loadConfiguration(const std::string & config_path)
{
    ConfigProcessor config_processor(config_path, true, true);
    ConfigProcessor::LoadedConfig config = config_processor.loadConfig(false);
    return config;
}

static Graphite::Params setGraphitePatternsFromConfigFile(const std::string & config_path)
{
    auto config = loadConfiguration(config_path);

    Graphite::Params params;
    setGraphitePatternsFromConfig(*config.configuration.get(), "graphite_rollup", params);

    return params;
}

typedef struct
{
    std::string path;
    size_t retention_index;
    size_t aggregation_index;
} patterns_for_path;


static std::vector<patterns_for_path> loadPatternsforPath(const std::string & metrics_file)
{
    std::vector<patterns_for_path> metrics;

    std::string str;
    std::ifstream in_stream;
    in_stream.open(metrics_file);
    if (!in_stream.is_open())
        throw std::runtime_error(metrics_file + " error: " + std::strerror(errno));

    while (1)
    {
        patterns_for_path p;
        in_stream >> p.path;
        if (in_stream.eof())
            break;
        if (in_stream.fail())
            throw std::runtime_error(metrics_file + " error: " + std::strerror(errno));
        in_stream >> p.retention_index;
        if (in_stream.eof())
            throw std::runtime_error(metrics_file + " error: unexpected eof");
        if (in_stream.fail())
            throw std::runtime_error(metrics_file + " error: " + std::strerror(errno));
        in_stream >> p.aggregation_index;
        if (in_stream.eof())
            throw std::runtime_error(metrics_file + " error: unexpected eof");
        if (in_stream.fail())
            throw std::runtime_error(metrics_file + " error: " + std::strerror(errno));

        metrics.push_back(p);
    }

    return metrics;
}

TEST(GraphiteTest, testSelectPattern)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::string config_path = SRC_DIR + "/src/Processors/Merges/Algorithms/tests/rollup.xml"s;
    std::string metrics_path = SRC_DIR + "/src/Processors/Merges/Algorithms/tests/rollup.txt"s;
    Graphite::Params params = setGraphitePatternsFromConfigFile(config_path);

    std::vector<patterns_for_path> tests = loadPatternsforPath(metrics_path);

    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        auto retention_want = params.patterns[t.retention_index];
        auto aggregation_want = params.patterns[t.aggregation_index];
        EXPECT_EQ(*rule.first, retention_want) << "retention rollup rule mismatch for " << t.path;
        EXPECT_EQ(*rule.second, aggregation_want) << "aggregation rollup mismatch rule for " << t.path;
    }
}

TEST(GraphiteTest, testSelectPatternTyped)
{
    tryRegisterAggregateFunctions();

    using namespace std::literals;

    std::string config_path = SRC_DIR + "/src/Processors/Merges/Algorithms/tests/rollup-typed.xml"s;
    std::string metrics_path = SRC_DIR + "/src/Processors/Merges/Algorithms/tests/rollup-typed.txt"s;
    Graphite::Params params = setGraphitePatternsFromConfigFile(config_path);

    std::vector<patterns_for_path> tests = loadPatternsforPath(metrics_path);

    for (const auto & t : tests)
    {
        auto rule = DB::Graphite::selectPatternForPath(params, t.path);
        auto retention_want = params.patterns[t.retention_index];
        auto aggregation_want = params.patterns[t.aggregation_index];
        EXPECT_EQ(*rule.first, retention_want) << "retention typed rollup rule mismatch for " << t.path;
        EXPECT_EQ(*rule.second, aggregation_want) << "aggregation typed rollup mismatch rule for " << t.path;
    }
}
