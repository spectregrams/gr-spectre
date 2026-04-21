/*
 * Copyright 2024-2026 Jimmy Fitzpatrick.
 * This file is part of SPECTRE
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#ifndef INCLUDED_SPECTRE_BATCHED_FILE_SINK_H
#define INCLUDED_SPECTRE_BATCHED_FILE_SINK_H

#include <gnuradio/spectre/api.h>
#include <gnuradio/sync_block.h>

#include <optional>

namespace gr {
namespace spectre {

/*!
 * \brief Writes the input stream to binary files in fixed-length batches.
 * \ingroup spectre
 *
 * \details Streams input samples as raw binary to files:
 *
 *     <timestamp>_<tag>.<input_type>
 *
 * where `<timestamp>` is the ISO 8601-formatted system time, `<tag>` is a user-defined
 * identifier and `<input_type>` specifies the data type (e.g., `fc32`). A new
 * file is opened every time a user-configured duration elapses. If the input
 * stream has stream tags, a corresponding metadata file can be created:
 *
 *     <timestamp>_<tag>.hdr
 *
 * which interleaves the tag values and the number of samples corresponding to that
 * tag, recording both as single precision floats.
 */
class SPECTRE_API batched_file_sink : virtual public gr::sync_block
{
public:
    typedef std::shared_ptr<batched_file_sink> sptr;

    /*!
     * \brief Make a batched file sink.
     *
     * \param dir Shared ancestral directory where output files will be stored.
     * \param tag Identifier included in the output file names.
     * \param input_type The data type of each sample in the input stream.
     * \param batch_size Duration (in seconds) to record samples from the input stream,
     * before creating a new file. \param sample_rate The sample rate of the input stream.
     * \param group_by_date If true, organise files using date-based subdirectories (e.g.,
     * `<year>/<month>/<day>`). \param is_tagged If true, metadata from stream tags is
     * recorded. \param tag_key Key used to extract values from stream tags if `is_tagged`
     * is true. \param initial_tag_value Default value used if no tag is present for the
     * first sample and `is_tagged` is true. 0 for not provided. \param start_time
     * Override the start time of the first batch, which by default is the current system
     * time at the first call to work.
     */
    static sptr
    make(const std::string& dir = ".",
         const std::string& tag = "spectre",
         const std::string& input_type = "fc32",
         const float batch_size = 1.0,
         const float sample_rate = 32000,
         const bool group_by_date = false,
         const bool is_tagged = false,
         const std::string& tag_key = "freq",
         const float initial_tag_value = 0,
         const std::optional<
             std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>>
             start_time = std::nullopt);
};

} // namespace spectre
} // namespace gr

#endif // INCLUDED_SPECTRE_BATCHED_FILE_SINK_H