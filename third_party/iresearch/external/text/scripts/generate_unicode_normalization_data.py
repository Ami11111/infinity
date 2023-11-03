#!/usr/bin/env python

# Copyright (C) 2020 T. Zachary Laine
#
# Distributed under the Boost Software License, Version 1.0. (See
# accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)

import lzw

cp_props_file_form = decls = '''\
// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/detail/normalization_data.hpp>

#include <boost/text/detail/lzw.hpp>

#include <boost/assert.hpp>

#include <unordered_map>


namespace boost {{ namespace text {{ namespace detail {{

    std::array<uint32_t, {1}> make_all_canonical_decompositions()
    {{
return {{{{
{0}
    }}}};
    }}

    std::array<uint32_t, {3}> make_all_compatible_decompositions()
    {{
return {{{{
{2}
    }}}};
    }}

    namespace {{
        constexpr std::array<uint16_t, {5}> data()
        {{
        return {{{{
{4}
        }}}};
        }}
    }}

    std::unordered_map<uint32_t, cp_props> make_cp_props_map()
    {{
        std::unordered_map<uint32_t, cp_props> retval;
        container::small_vector<unsigned char, 256> buf;
        auto const & compressed = data();
        lzw_decompress(
            compressed.begin(),
            compressed.end(),
            lzw_to_cp_props_iter(retval, buf));
        BOOST_ASSERT(buf.empty());
        BOOST_ASSERT(retval.size() == {6});
        return retval;
    }}

}}}}}}
'''

compose_file_form = decls = '''\
// Copyright (C) 2020 T. Zachary Laine
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

// Warning! This file is autogenerated.
#include <boost/text/detail/normalization_data.hpp>

#include <array>
#include <unordered_map>


namespace boost {{ namespace text {{ namespace detail {{

    namespace {{
        struct data_t {{ uint64_t key_; uint32_t value_; }};
        constexpr std::array<data_t, {1}> data()
        {{
        return {{{{
{0}
        }}}};
        }}
    }}

    std::unordered_map<uint64_t, uint32_t> make_composition_map()
    {{
        std::unordered_map<uint64_t, uint32_t> retval;
        for (auto datum : data()) {{
            retval[datum.key_] = datum.value_;
        }}
        return retval;
    }}

}}}}}}
'''


def cccs(filename):
    intervals = []
    lines = open(filename, 'r').readlines()
    for line in lines:
        line = line[:-1]
        if not line.startswith('#') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            fields = map(lambda x: x.strip(), line.split(';'))
            ccc = fields[1]
            if ccc == '0':
                continue
            code_points = fields[0]
            if '..' in code_points:
                cps = code_points.split('.')
                interval = (int(cps[0], 16), int(cps[2], 16) + 1, ccc)
            else:
                cp = int(code_points, 16)
                interval = (cp, cp + 1, ccc)
            intervals.append(interval)

    intervals = sorted(intervals)
    intervals_list = ''
    cccs_dict = {}
    num_intervals = 0
    for interval in intervals:
        if False: #128 < interval[1] - interval[0]:
            num_intervals += 1
            intervals_list += '    ccc_interval{{{}, {}, {}}},\n'.format(
                hex(interval[0]), hex(interval[1]), interval[2]
            )
        else:
            for i in range(interval[0], interval[1]):
                cccs_dict[i] = interval[2]
    return cccs_dict

def expand_decomp_canonical(decomp, all_decomps):
    first_cp = decomp[0][0]
    if decomp[1] and first_cp in all_decomps and all_decomps[first_cp][1]:
        return expand_decomp_canonical((all_decomps[first_cp][0] + decomp[0][1:], True), all_decomps)
    return decomp

def expand_decomp_compatible(decomp, all_decomps):
    expandables = len(filter(lambda cp: cp in all_decomps, decomp[0]))
    if 0 < expandables:
        cps = map(lambda cp: cp in all_decomps and all_decomps[cp][0] or cp, decomp[0])
        flat_cps = []
        for cp in cps:
            if type(cp) == list:
                flat_cps += cp
            else:
                flat_cps.append(cp)
        return expand_decomp_compatible((flat_cps, True), all_decomps)
    return decomp

def get_decompositions(filename, cccs_dict, expand_decomp, canonical_only):
    decomps = {}

    # Pass 1: Find top-level decompositions
    lines = open(filename, 'r').readlines()
    for line in lines:
        line = line[:-1]
        if not line.startswith('#') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            fields = map(lambda x: x.strip(), line.split(';'))
            if fields[5] == '':
                continue
            decomp_cps = fields[5].split(' ')
            canonical = True
            if decomp_cps[0].startswith('<'):
                decomp_cps = decomp_cps[1:]
                canonical = False
            decomp_cps = map(lambda x: int(x, 16), decomp_cps)
            cp = int(fields[0], 16)
            decomp = (decomp_cps, canonical)
            decomps[cp] = decomp

    # Pass 2: Expand Pass-1 decompositions
    expanded_decomps = map(
        lambda item: (item[0], expand_decomp(item[1], decomps)),
        sorted(decomps.items())
    )

    # Pass 3: Reorder Pass-2 decompositions uing cccs
    def reorder_cps(cps, cccs_dict):
        for i in range(1, len(cps) - 1):
            cp_a = cps[i]
            cp_b = cps[i + 1]
            # Each must be nonzero....
            if cp_a not in cccs_dict or cp_b not in cccs_dict:
                continue
            ccc_a = cccs_dict[cp_a]
            ccc_b = cccs_dict[cp_b]
            if ccc_b < ccc_a:
                cps[i] = cp_b
                cps[i + 1] = cp_a
        return cps

    decomps = map(
        lambda x: (x[0], (reorder_cps(x[1][0], cccs_dict), x[1][1])),
        expanded_decomps
    )

    all_cps = []
    final_decomps = []
    for k,v in decomps:
        if canonical_only and not v[1]:
            continue
        first = len(all_cps)
        all_cps += v[0]
        last = len(all_cps)
        final_decomps.append((k, (first, last)))

    return (all_cps, final_decomps)


def get_compositions(filename, derived_normalization_props_filename):
    comps = []

    exclusions = set()
    lines = open(derived_normalization_props_filename, 'r').readlines()
    for line in lines:
        line = line[:-1]
        if not line.startswith('#') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            fields = map(lambda x: x.strip(), line.split(';'))
            prop = fields[1]
            if prop != 'Full_Composition_Exclusion':
                continue
            code_points = fields[0]
            if '..' in code_points:
                cps = code_points.split('.')
                interval = (int(cps[0], 16), int(cps[2], 16) + 1)
            else:
                cp = int(code_points, 16)
                interval = (cp, cp + 1)
            for cp in range(interval[0], interval[1]):
                exclusions.add(cp)

    lines = open(filename, 'r').readlines()
    for line in lines:
        line = line[:-1]
        if not line.startswith('#') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            fields = map(lambda x: x.strip(), line.split(';'))
            if fields[5] == '':
                continue
            decomp_cps = fields[5].split(' ')
            if len(decomp_cps) != 2 or decomp_cps[0].startswith('<'):
                continue
            decomp_cps = map(lambda x: int(x, 16), decomp_cps)
            cp = int(fields[0], 16)
            if cp not in exclusions:
                comps.append((decomp_cps, cp))

    return sorted(comps)

def get_quick_checks(filename):
    retval = {'NFD': {}, 'NFC': {}, 'NFKD': {}, 'NFKC': {}}

    file_prop_to_enum = {'N': 'quick_check::no', 'M': 'quick_check::maybe'}

    lines = open(filename, 'r').readlines()
    for line in lines:
        line = line[:-1]
        if not line.startswith('#') and len(line) != 0:
            comment_start = line.find('#')
            comment = ''
            if comment_start != -1:
                comment = line[comment_start + 1:].strip()
                line = line[:comment_start]
            fields = map(lambda x: x.strip(), line.split(';'))
            prop = fields[1]
            if not prop.endswith('_QC'):
                continue
            normalization_form = prop.split('_')[0]
            code_points = fields[0]
            if '..' in code_points:
                cps = code_points.split('.')
                interval = (int(cps[0], 16), int(cps[2], 16) + 1)
            else:
                cp = int(code_points, 16)
                interval = (cp, cp + 1)
            for cp in range(interval[0], interval[1]):
                retval[normalization_form][cp] = file_prop_to_enum[fields[2]]

    return retval

if __name__ == "__main__":
    composition_mapping = get_compositions('UnicodeData.txt', 'DerivedNormalizationProps.txt')
    item_strings = map(
        lambda x : 'key({}, {}), {}'.format(hex(x[0][0]), hex(x[0][1]), hex(x[1])),
        composition_mapping
    )
    compositions_map = '    { ' + ' },\n    { '.join(item_strings) + ' },\n'

    cpp_file = open('normalization_data_compose.cpp', 'w')
    cpp_file.write(compose_file_form.format(compositions_map, len(item_strings)))


    cccs_dict = cccs('DerivedCombiningClass.txt')

    (canon_all_cps, canon_decomposition_mapping) = \
      get_decompositions('UnicodeData.txt', cccs_dict, expand_decomp_canonical, True)
    canon_decomposition_mapping = dict(canon_decomposition_mapping)
    (compat_all_cps, compat_decomposition_mapping) = \
      get_decompositions('UnicodeData.txt', cccs_dict, expand_decomp_compatible, False)
    compat_decomposition_mapping = dict(compat_decomposition_mapping)

    canon_all_cps_string = '    ' + ',\n    '.join(map(lambda x: hex(x), canon_all_cps)) + ',\n'
    compat_all_cps_string = '    ' + ',\n    '.join(map(lambda x: hex(x), compat_all_cps)) + ',\n'

    quick_check_maps = get_quick_checks('DerivedNormalizationProps.txt')

    all_cps = set()
    for cp in cccs_dict:
        all_cps.add(cp)
    for cp in canon_decomposition_mapping:
        all_cps.add(cp)
    for cp in compat_decomposition_mapping:
        all_cps.add(cp)
    for k,v in quick_check_maps.items():
        for cp in v:
            all_cps.add(cp)

    def decomp_str(decomp):
        return '{{{}, {}}}'.format(decomp[0], decomp[1])
    def cast(quick_check):
        return '(uint8_t)' + quick_check
    def quick_checks_to_byte(quick_check_1, quick_check_2):
        enum_value = {
            'quick_check::yes': 0,
            'quick_check::no': 1,
            'quick_check::maybe': 2
        }
        return enum_value[quick_check_1] << 4 | enum_value[quick_check_2]

    prop_bytes_ = []
    for cp in sorted(all_cps):
        canonical_decomp = (0, 0)
        if cp in canon_decomposition_mapping:
            canonical_decomp = canon_decomposition_mapping[cp]
        compatible_decomp = (0, 0)
        if cp in compat_decomposition_mapping:
            compatible_decomp = compat_decomposition_mapping[cp]
        ccc = 0
        if cp in cccs_dict:
            ccc = cccs_dict[cp]
        nfd_quick_check = 'quick_check::yes'
        if cp in quick_check_maps['NFD']:
            nfd_quick_check = quick_check_maps['NFD'][cp]
        nfkd_quick_check = 'quick_check::yes'
        if cp in quick_check_maps['NFKD']:
            nfkd_quick_check = quick_check_maps['NFKD'][cp]
        nfc_quick_check = 'quick_check::yes'
        if cp in quick_check_maps['NFC']:
            nfc_quick_check = quick_check_maps['NFC'][cp]
        nfkc_quick_check = 'quick_check::yes'
        if cp in quick_check_maps['NFKC']:
            nfkc_quick_check = quick_check_maps['NFKC'][cp]
        lzw.add_cp(prop_bytes_, cp)
        lzw.add_short(prop_bytes_, canonical_decomp[0])
        lzw.add_short(prop_bytes_, canonical_decomp[1])
        lzw.add_short(prop_bytes_, compatible_decomp[0])
        lzw.add_short(prop_bytes_, compatible_decomp[1])
        lzw.add_byte(prop_bytes_, int(ccc))
        lzw.add_byte(prop_bytes_, \
                     quick_checks_to_byte(nfd_quick_check, nfkd_quick_check))
        lzw.add_byte(prop_bytes_, \
                     quick_checks_to_byte(nfc_quick_check, nfkc_quick_check))

    value_per_line = 12
    compressed_bytes = lzw.compress(prop_bytes_)
    props_lines, num_shorts = lzw.compressed_bytes_to_lines(compressed_bytes, value_per_line)
    #print 'rewrote {} * 144 = {} bits as {} * 8 = {} bits'.format(len(all_cps), len(all_cps)*144, len(prop_bytes_), len(prop_bytes_)*8)
    #print 'compressed to {} * 16 = {} bits'.format(len(compressed_bytes), len(compressed_bytes) * 16)

    cpp_file = open('normalization_data_cp_props.cpp', 'w')
    cpp_file.write(cp_props_file_form.format(
        canon_all_cps_string, len(canon_all_cps),
        compat_all_cps_string, len(compat_all_cps),
        props_lines, num_shorts,
        len(all_cps)
    ))


def cps_string(cps):
    cps = map(lambda x: hex(x)[2:], cps)
    return ''.join(map(lambda x: r'\U' + '0' * (8 - len(x)) + x, cps))

def cus_lines(cus):
    as_ints = map(lambda x: ord(x), cus)
    values_per_line = 12
    return lzw.compressed_bytes_to_lines(as_ints, values_per_line)[0]

def utf8_cps(cps):
    s = cps_string(cps)
    exec("s = u'" + s + "'")
    s = s.encode('UTF-8', 'strict')
    retval = cus_lines(s)
    print 'rewrote {} * 32 = {} bits as {} * 8 = {} bits ({} bytes saved)'.format(len(cps), len(cps) * 32, len(s), len(s) * 8, (len(cps) * 32 - len(s) * 8) / 8.0)
    return retval

#utf8_cps(canon_all_cps)
#utf8_cps(compat_all_cps)