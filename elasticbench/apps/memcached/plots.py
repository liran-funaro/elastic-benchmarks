"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
from cloudexp.results.analyze import fetch_data
from cloudexp.results.data import ExpData
from cloudexp.results.plots import RATE, PERCENT, MEMORY, PLOT_PERIOD, PLOT_FUTURE_MOMENT, MB_RATE


def perf_hit_rate(d: ExpData):
    return fetch_data(d, y=('perf:get:period:ops', 'perf:get:period:get_miss', 'perf:get:period:time(s)'),
                      out='hit-rate',
                      out_func=lambda df: df['perf:get:period:ops'].subtract(df['perf:get:period:get_miss']).divide(
                          df['perf:get:period:time(s)']),
                      y_name=RATE, **PLOT_PERIOD)


def perf_hit_percentage(d: ExpData):
    return fetch_data(d, y=('perf:get:period:ops', 'perf:get:period:get_miss', 'perf:get:period:time(s)'),
                      out='hit-percentage',
                      out_func=lambda df: 100 * df['perf:get:period:ops'].subtract(
                          df['perf:get:period:get_miss']).divide(
                          df['perf:get:period:ops']),
                      y_name=PERCENT, **PLOT_PERIOD)


def perf_miss_rate(d: ExpData):
    return fetch_data(d, y=('perf:get:period:get_miss', 'perf:get:period:time(s)'),
                      out='miss-rate',
                      out_func=lambda df: df['perf:get:period:get_miss'].divide(df['perf:get:period:time(s)']),
                      y_name=RATE, **PLOT_PERIOD)


def perf_total_throughput(d: ExpData):
    return fetch_data(d, y='perf:total:period:tps(ops/s)', y_name=RATE, **PLOT_PERIOD)


def perf_get_throughput(d: ExpData):
    return fetch_data(d, y='perf:get:period:tps(ops/s)', y_name=RATE, **PLOT_PERIOD)


def perf_set_throughput(d: ExpData):
    return fetch_data(d, y='perf:set:period:tps(ops/s)', y_name=RATE, **PLOT_PERIOD)


def perf_get_net_rate(d: ExpData):
    return fetch_data(d, y='perf:get:global:net(m/s)', y_name=MB_RATE, **PLOT_PERIOD)


def perf_set_net_rate(d: ExpData):
    return fetch_data(d, y='perf:set:global:net(m/s)', y_name=MB_RATE, **PLOT_PERIOD)


def perf_ops(d: ExpData):
    return fetch_data(d, y='perf:get:period:ops', y_name=RATE, **PLOT_PERIOD)


def perf_hit_rate_resample(d: ExpData):
    return fetch_data(d, y=('perf:get:period:ops', 'perf:get:period:get_miss', 'perf:get:period:time(s)'),
                      out='hit-rate-window',
                      out_func=lambda df: df['perf:get:period:ops'].subtract(df['perf:get:period:get_miss']).divide(
                          df['perf:get:period:time(s)']),
                      resample=(300, 1),
                      y_name=RATE, **PLOT_PERIOD)


def perf_get_throughput_resample(d: ExpData):
    return fetch_data(d, y='perf:get:period:tps(ops/s)', y_name=RATE, resample=(300, 1), **PLOT_PERIOD)


MEMCACHED_KEYS = [
    'uptime', 'time', 'pointer_size', 'rusage_user', 'rusage_system', 'max_connections', 'curr_connections',
    'total_connections', 'rejected_connections', 'connection_structures', 'reserved_fds', 'cmd_get', 'cmd_set',
    'cmd_flush', 'cmd_touch', 'get_hits', 'get_misses', 'get_expired', 'get_flushed', 'delete_misses',
    'delete_hits', 'incr_misses', 'incr_hits', 'decr_misses', 'decr_hits', 'cas_misses', 'cas_hits',
    'cas_badval', 'touch_hits', 'touch_misses', 'auth_cmds', 'auth_errors', 'bytes_read', 'bytes_written',
    'limit_maxbytes', 'accepting_conns', 'listen_disabled_num', 'time_in_listen_disabled_us', 'threads',
    'conn_yields', 'hash_power_level', 'hash_bytes', 'hash_is_expanding', 'slab_reassign_rescues',
    'slab_reassign_chunk_rescues', 'slab_reassign_evictions_nomem', 'slab_reassign_inline_reclaim',
    'slab_reassign_busy_items', 'slab_reassign_busy_deletes', 'slab_reassign_running', 'slabs_moved',
    'lru_crawler_running', 'lru_crawler_starts', 'lru_maintainer_juggles', 'malloc_fails', 'log_worker_dropped',
    'log_worker_written', 'log_watcher_skipped', 'log_watcher_sent', 'bytes', 'curr_items', 'total_items',
    'slab_global_page_pool', 'expired_unfetched', 'evicted_unfetched', 'evicted_active', 'evictions',
    'reclaimed', 'crawler_reclaimed', 'crawler_items_checked', 'lrutail_reflocked', 'moves_to_cold',
    'moves_to_warm', 'moves_within_lru', 'direct_reclaims', 'lru_bumps_dropped'
]


def memcached_stats(key: str):
    def fetch_memcached_stats(d: ExpData):
        return fetch_data(d, y=f'memcached:{key}', y_name=key, **PLOT_FUTURE_MOMENT)
    return fetch_memcached_stats


def mem_alloc(d: ExpData):
    return fetch_data(d, y='memcached:total_malloced', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_rss(d: ExpData):
    return fetch_data(d, y='memcached:rss', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def memaslap_rss(d: ExpData):
    return fetch_data(d, y='memaslap-stats:memory:rss', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def memaslap_vsize(d: ExpData):
    return fetch_data(d, y='memaslap-stats:memory:vsize', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


def mem_max_bytes(d: ExpData):
    return fetch_data(d, y='memcached:limit_maxbytes', y_name=MEMORY, **PLOT_FUTURE_MOMENT)


# Used by ExpAnalyzer
perf = perf_hit_rate
memory = mem_alloc
perf_window = 90
