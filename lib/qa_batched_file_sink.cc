
/*
 * Copyright 2024-2026 Jimmy Fitzpatrick.
 * This file is part of SPECTRE
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/blocks/vector_source.h>
#include <gnuradio/spectre/batched_file_sink.h>
#include <gnuradio/tags.h>
#include <gnuradio/top_block.h>
#include <pmt/pmt.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <utility>

namespace {
gr::tag_t
make_frequency_tag(const uint64_t offset, const std::string& key, const double value)
{
    gr::tag_t tag;
    tag.offset = offset;
    tag.key = pmt::string_to_symbol(key);
    tag.value = pmt::from_double(value);
    return tag;
}
} // namespace

/* Run a flowgraph that writes 20 complex floats (without stream tags) to disk in 4
 * batches, with the first sample timestamped at the Unix epoch:
 *
 * offset:  0     1     2     ... 19
 * sample:  1+0i  1+0i  1+0i  ... 1+0i
 *
 * We then verify each file is timestamped correctly and that we can recover the samples.
 *
 * Expected output:
 *   1970-01-01T00:00:00Z000000_foo.fc32  -- samples 0..4
 *   1970-01-01T00:00:01Z000000_foo.fc32  -- samples 5..9
 *   1970-01-01T00:00:02Z000000_foo.fc32  -- samples 10..14
 *   1970-01-01T00:00:03Z000000_foo.fc32  -- samples 15..19
 */
BOOST_AUTO_TEST_CASE(batched_file_sink_run)
{
    namespace fs = std::filesystem;

    const fs::path tmpdir = fs::temp_directory_path() / "batched_file_sink_run";
    fs::create_directories(tmpdir);

    const size_t total_samples = 20;
    auto source = gr::blocks::vector_source_c::make(
        std::vector<gr_complex>(total_samples, { 1, 0 }));

    auto sink = gr::spectre::batched_file_sink::make(
        tmpdir,
        /*tag=*/"foo",
        /*input_type=*/"fc32",
        /*batch_size=*/1.0f,
        /*sample_rate=*/5.0f,
        /*group_by_date=*/false,
        /*is_tagged=*/false,
        /*tag_key=*/"unused",
        /*initial_tag_value=*/0.0f,
        /*start_time=*/std::chrono::system_clock::from_time_t(0));

    auto tb = gr::make_top_block("top");
    tb->connect(source, 0, sink, 0);
    tb->run();

    std::vector<fs::path> paths = { tmpdir / "1970-01-01T00:00:00.000000Z_foo.fc32",
                                    tmpdir / "1970-01-01T00:00:01.000000Z_foo.fc32",
                                    tmpdir / "1970-01-01T00:00:02.000000Z_foo.fc32",
                                    tmpdir / "1970-01-01T00:00:03.000000Z_foo.fc32" };
    const int num_samples_per_batch = 5;
    const std::vector<gr_complex> expected(num_samples_per_batch, { 1, 0 });
    for (const auto& path : paths) {
        BOOST_TEST(fs::exists(path));
        std::ifstream f(path, std::ios::binary);
        std::vector<gr_complex> buff(num_samples_per_batch, { 0, 0 });
        f.read(reinterpret_cast<char*>(buff.data()),
               num_samples_per_batch * sizeof(gr_complex));
        BOOST_TEST(buff == expected);
    }

    fs::remove_all(tmpdir);
}

/**
 * Run a flowgraph which writes 10 complex floats (with stream tags) to disk in 2
 * batches, with the first sample timestamped at the Unix epoch:
 *
 * Stream:
 * offset:  0     1     2     3     4     5     6     7     8     9
 * sample:  1+0i  1+0i  1+0i  1+0i  1+0i  1+0i  1+0i  1+0i  1+0i  1+0i
 * tags:    f0    f1          f2                  f3          f4
 *
 * We then verify each file is timestamped correctly and that we can recover the samples
 * and stream tags.
 *
 * Expected output:
 *   1970-01-01T00:00:00Z000000_foo.fc32  -- samples 0..4
 *   1970-01-01T00:00:00Z000000_foo.hdr   -- (f0, 1), (f1, 2), (f2, 2)
 *   1970-01-01T00:00:01Z000000_foo.fc32  -- samples 5..9
 *   1970-01-01T00:00:01Z000000_foo.hdr   -- (f2, 1), (f3, 2), (f4, 2)
 */
BOOST_AUTO_TEST_CASE(batched_file_sink_stream_tags)
{
    namespace fs = std::filesystem;
    typedef std::pair<float, uint64_t> stream_tag;

    const fs::path tmpdir = fs::temp_directory_path() / "batched_file_sink_stream_tags";
    fs::create_directories(tmpdir);

    const std::string tag_key = "freq";
    const size_t total_samples = 10;
    std::vector<gr::tag_t> tags = { make_frequency_tag(0, tag_key, 90e6),
                                    make_frequency_tag(1, tag_key, 95e6),
                                    make_frequency_tag(3, tag_key, 100e6),
                                    make_frequency_tag(6, tag_key, 105e6),
                                    make_frequency_tag(8, tag_key, 110e6) };
    std::vector<gr_complex> input(total_samples, { 1, 0 });
    auto source =
        gr::blocks::vector_source_c::make(input, /*repeat=*/false, /*vlen=*/1, tags);

    auto sink = gr::spectre::batched_file_sink::make(
        /*dir=*/tmpdir,
        /*tag=*/"foo",
        /*input_type=*/"fc32",
        /*batch_size=*/1.0f,
        /*sample_rate=*/5.0f,
        /*group_by_date=*/false,
        /*is_tagged=*/true,
        /*tag_key=*/tag_key,
        /*initial_tag_value=*/0.0f,
        /*start_time=*/std::chrono::system_clock::from_time_t(0));

    auto tb = gr::make_top_block("top");
    tb->connect(source, 0, sink, 0);
    tb->run();

    std::vector<fs::path> data_paths = { tmpdir / "1970-01-01T00:00:00.000000Z_foo.fc32",
                                         tmpdir /
                                             "1970-01-01T00:00:01.000000Z_foo.fc32" };
    const int num_samples_per_batch = 5;
    const std::vector<gr_complex> expected_data(num_samples_per_batch, gr_complex(1, 0));

    for (const fs::path& path : data_paths) {
        BOOST_TEST(fs::exists(path));
        std::ifstream f(path, std::ios::binary);
        std::vector<gr_complex> got_data(num_samples_per_batch);
        f.read(reinterpret_cast<char*>(got_data.data()),
               sizeof(gr_complex) * num_samples_per_batch);
        BOOST_TEST(expected_data == got_data);
    }

    std::vector<fs::path> hdr_paths = { tmpdir / "1970-01-01T00:00:00.000000Z_foo.hdr",
                                        tmpdir / "1970-01-01T00:00:01.000000Z_foo.hdr" };
    const int num_tags_per_batch = 3;
    const std::vector<stream_tag> expected_tags = {
        stream_tag(90e6, 1),  stream_tag(95e6, 2),  stream_tag(100e6, 2),
        stream_tag(100e6, 1), stream_tag(105e6, 2), stream_tag(110e6, 2)
    };
    std::vector<stream_tag> got_tags;
    got_tags.reserve(hdr_paths.size() * num_tags_per_batch);
    for (const fs::path& path : hdr_paths) {
        BOOST_TEST(fs::exists(path));
        std::ifstream f(path, std::ios::binary);
        stream_tag tag;
        for (size_t n = 0; n < num_tags_per_batch; n++) {
            f.read(reinterpret_cast<char*>(&tag.first), sizeof(stream_tag::first_type));
            f.read(reinterpret_cast<char*>(&tag.second), sizeof(stream_tag::second_type));
            got_tags.push_back(tag);
        }
    }
    BOOST_TEST(expected_tags == got_tags);

    fs::remove_all(tmpdir);
}