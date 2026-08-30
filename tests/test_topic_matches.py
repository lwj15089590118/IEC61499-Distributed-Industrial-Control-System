# -*- coding: utf-8 -*-
"""topic_matches（MQTT 风格通配符匹配）全覆盖用例。"""

from __future__ import annotations

from communication.event_bus import topic_matches


class TestExact:
    """精确匹配（无通配符）。"""

    def test_exact_hit(self):
        assert topic_matches("factory/events", "factory/events")

    def test_exact_miss(self):
        assert not topic_matches("factory/events", "factory/alerts")

    def test_prefix_is_not_match(self):
        # 主题必须逐层完整匹配，前缀相同但多一层不算命中
        assert not topic_matches("factory/events", "factory/events/extra")

    def test_pattern_longer_than_topic(self):
        assert not topic_matches("factory/events/extra", "factory/events")


class TestSingleLevelWildcard:
    """'+' 单层通配符。"""

    def test_plus_matches_one_level(self):
        assert topic_matches("factory/tasks/+", "factory/tasks/node_b")
        assert topic_matches("factory/tasks/+", "factory/tasks/node_c")

    def test_plus_does_not_cross_levels(self):
        assert not topic_matches("factory/tasks/+", "factory/tasks/node_b/sub")

    def test_plus_requires_the_level(self):
        assert not topic_matches("factory/tasks/+", "factory/tasks")

    def test_plus_in_middle(self):
        assert topic_matches("factory/heartbeat/+", "factory/heartbeat/node_a")
        assert not topic_matches("factory/heartbeat/+", "factory/orders/node_a")

    def test_plus_alone(self):
        assert topic_matches("+", "anything")
        assert not topic_matches("+", "two/levels")


class TestMultiLevelWildcard:
    """'#' 多层通配符。"""

    def test_hash_matches_deep_suffix(self):
        assert topic_matches("factory/#", "factory/anything/deep")
        assert topic_matches("factory/#", "factory")

    def test_hash_matches_single_level(self):
        assert topic_matches("factory/heartbeat/#", "factory/heartbeat/node_a")

    def test_hash_prefix_must_match(self):
        assert not topic_matches("factory/heartbeat/#", "factory/orders/new")

    def test_hash_alone(self):
        assert topic_matches("#", "any/topic/at/all")


class TestEdgeCases:
    """边界条件。"""

    def test_empty_inputs(self):
        assert not topic_matches("", "factory/events")
        assert not topic_matches("factory/events", "")
        assert not topic_matches("", "")

    def test_topic_shorter_than_pattern(self):
        assert not topic_matches("a/b/c", "a/b")

    def test_mismatch_at_last_level(self):
        assert not topic_matches("factory/tasks/node_b", "factory/tasks/node_c")

    def test_wildcard_plus_and_hash_combined(self):
        assert topic_matches("factory/+/node_a/#", "factory/heartbeat/node_a/x/y")
        assert not topic_matches("factory/+/node_a/#", "factory/heartbeat/node_b/x")
