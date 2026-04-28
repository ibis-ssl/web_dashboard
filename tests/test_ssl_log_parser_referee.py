import gzip
import pathlib
import struct
import sys
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import ssl_log_parser
from state import ssl_gc_referee_message_pb2


def _set_team(team, name, score):
    team.name = name
    team.score = score
    team.red_cards = 0
    team.yellow_cards = 0
    team.timeouts = 0
    team.timeout_time = 0
    team.goalkeeper = 0


def _referee(yellow_name, blue_name, yellow_score, blue_score, stage=1, command=1, counter=1):
    ref = ssl_gc_referee_message_pb2.Referee()
    ref.packet_timestamp = counter
    ref.stage = stage
    ref.command = command
    ref.command_counter = counter
    ref.command_timestamp = counter
    _set_team(ref.yellow, yellow_name, yellow_score)
    _set_team(ref.blue, blue_name, blue_score)
    return ref


def _log_bytes(*entries):
    data = bytearray(ssl_log_parser.SSL_LOG_HEADER)
    data.extend(struct.pack(">i", 1))
    for timestamp_ns, ref in entries:
        payload = ref.SerializeToString()
        data.extend(
            struct.pack(
                ">qii",
                timestamp_ns,
                ssl_log_parser.MSG_TYPE_REFEREE,
                len(payload),
            )
        )
        data.extend(payload)
    return gzip.compress(bytes(data))


class RefereeSummaryTest(unittest.TestCase):
    def test_ignores_unknown_zero_terminal_reset(self):
        log = _log_bytes(
            (0, _referee("ZUNOH Robotics", "ibis", 0, 0, counter=1)),
            (1_000_000_000, _referee("ZUNOH Robotics", "ibis", 0, 1, counter=2)),
            (2_000_000_000, _referee("ZUNOH Robotics", "ibis", 0, 6, counter=3)),
            (3_000_000_000, _referee("Unknown", "Unknown", 0, 0, stage=0, command=0, counter=4)),
        )

        analysis = ssl_log_parser.extract_full_analysis(
            log,
            filename="2026-04-25_02-01_GROUP_PHASE_ZUNOH_Robotics-vs-ibis_SSL01.log.gz",
        )

        self.assertEqual(analysis["meta"]["teams"], {"yellow": "ZUNOH Robotics", "blue": "ibis"})
        self.assertEqual(analysis["meta"]["final_score"], {"yellow": 0, "blue": 6})
        self.assertEqual(analysis["score_timeline"][-1], {"t_sec": 2.0, "yellow": 0, "blue": 6})

    def test_keeps_zero_zero_known_match_after_reset(self):
        log = _log_bytes(
            (0, _referee("GreenTea", "Ri-one", 0, 0, counter=1)),
            (1_000_000_000, _referee("Unknown", "Unknown", 0, 0, stage=0, command=0, counter=2)),
        )

        analysis = ssl_log_parser.extract_full_analysis(log)

        self.assertEqual(analysis["meta"]["teams"], {"yellow": "GreenTea", "blue": "Ri-one"})
        self.assertEqual(analysis["meta"]["final_score"], {"yellow": 0, "blue": 0})
        self.assertEqual(analysis["score_timeline"], [{"t_sec": -0.0, "yellow": 0, "blue": 0}])

    def test_uses_filename_team_names_when_referee_names_are_unknown(self):
        log = _log_bytes(
            (0, _referee("Unknown", "Unknown", 0, 0, counter=1)),
        )

        analysis = ssl_log_parser.extract_full_analysis(
            log,
            filename="2026-04-26_01-44_GROUP_PHASE_Ri-one-vs-ZUNOH_Robotics_SSL10.log.gz",
        )

        self.assertEqual(analysis["meta"]["teams"], {"yellow": "Ri-one", "blue": "ZUNOH Robotics"})
        self.assertEqual(analysis["meta"]["final_score"], {"yellow": 0, "blue": 0})

    def test_falls_back_to_colors_without_referee_or_filename_identity(self):
        log = _log_bytes(
            (0, _referee("Unknown", "Unknown", 0, 0, counter=1)),
        )

        analysis = ssl_log_parser.extract_full_analysis(log, filename="unknown.log.gz")

        self.assertEqual(analysis["meta"]["teams"], {"yellow": "Yellow", "blue": "Blue"})
        self.assertEqual(analysis["meta"]["final_score"], {"yellow": 0, "blue": 0})


if __name__ == "__main__":
    unittest.main()
