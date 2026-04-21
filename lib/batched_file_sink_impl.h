/*
 * Copyright 2024-2026 Jimmy Fitzpatrick.
 * This file is part of SPECTRE
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_SPECTRE_BATCHED_FILE_SINK_IMPL_H
#define INCLUDED_SPECTRE_BATCHED_FILE_SINK_IMPL_H

#include <gnuradio/spectre/batched_file_sink.h>

#include <gnuradio/types.h>
#include <filesystem>
#include <fstream>
#include <optional>

namespace gr {
namespace spectre {

struct batch_time {
    std::tm utc_tm;
    int us;
};

enum buffer_state {
    EMPTY = 0,
    FILLING,
    FULL,
};

class batched_file_sink_impl : public batched_file_sink
{
public:
    batched_file_sink_impl(
        const std::string& dir,
        const std::string& tag,
        const std::string& input_type,
        const float batch_size,
        const float sample_rate,
        const bool group_by_date,
        const bool is_tagged,
        const std::string& tag_key,
        const float initial_tag_value,
        const std::optional<
            std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>
            start_time);
    ~batched_file_sink_impl();
    int work(int noutput_items,
             gr_vector_const_void_star& in,
             gr_vector_void_star& out) override;

private:
    const std::string d_dir;
    const std::string d_tag;
    const std::string d_input_type;
    const float d_sample_rate;
    const size_t d_sizeof_stream_item;
    const int d_nsamples_per_batch;
    const bool d_is_tagged;
    const bool d_group_by_date;
    const pmt::pmt_t d_tag_key;
    const float d_initial_tag_value;
    buffer_state d_buffer_state;

    // Timestamp batches assuming a constant sample rate.
    batch_time d_batch_time;
    std::optional<
        std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>
        d_start_time;
    int d_nsamples;

    // Data buffer.
    int d_nbuffered_samples;
    std::vector<char> d_data_buffer;
    std::ofstream d_fdata;

    // Tag buffer.
    int d_nbuffered_tags;
    std::vector<float> d_tags_buffer;
    std::ofstream d_ftags;
    tag_t d_active_tag;


    void init(std::chrono::nanoseconds time_elapsed);
    void flush();

    void set_batch_time(std::chrono::nanoseconds time_elapsed);

    void open_fstream(std::ofstream& f, const std::string& extension);
    void open_fstreams();
    void close_fstreams();

    int fill_data_buffer(int noutput_items, const char* in);
    void flush_data_buffer();

    std::optional<tag_t> get_tag_from_first_sample();
    bool tag_is_set() const;
    void set_initial_active_tag();
    void fill_tag_buffer(int nconsumed);
    void flush_tag_buffer();
};

} // namespace spectre
} // namespace gr

#endif