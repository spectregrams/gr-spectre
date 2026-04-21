
/*
 * Copyright 2024-2026 Jimmy Fitzpatrick.
 * This file is part of SPECTRE
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gnuradio/blocks/vector_source.h>
#include <gnuradio/spectre/batched_file_sink.h>
#include <gnuradio/top_block.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>

/* Run a flowgraph that writes 20 complex float samples to disk in 4 batches, with the
 * first sample timestamped at the Unix epoch. We then verify each file is timestamped
   correctly and that we can recover the samples. */
BOOST_AUTO_TEST_CASE(batched_file_sink_run)
{
    namespace fs = std::filesystem;

    const fs::path tmpdir = fs::temp_directory_path() / "batched_file_sink";
    fs::create_directories(tmpdir);

    const int total_samples = 20;
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
    const int samples_per_batch = 5;
    const std::vector<gr_complex> expected(samples_per_batch, { 1, 0 });
    for (const auto& path : paths) {
        BOOST_TEST(fs::exists(path));
        std::ifstream f(path, std::ios::binary);
        std::vector<gr_complex> buff(samples_per_batch, { 0, 0 });
        f.read(reinterpret_cast<char*>(buff.data()),
               samples_per_batch * sizeof(gr_complex));
        BOOST_TEST(buff == expected);
    }

    fs::remove_all(tmpdir);
}