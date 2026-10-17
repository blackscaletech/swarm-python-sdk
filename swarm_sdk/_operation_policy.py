from ._operation_policies import CLIENT_OPERATION_POLICIES


_POLICIES = [(method, [(segment[:segment.index('{')], segment[segment.index('}') + 1:]) if '{' in segment else segment
                      for segment in path.split('/')], required, strategy)
             for method, path, required, strategy in CLIENT_OPERATION_POLICIES]


def _client_operation_key_strategy(method, path, explicit_strategy=None):
    if explicit_strategy is not None and explicit_strategy not in {'fresh-attempt', 'durable-content'}:
        raise ValueError('Invalid client idempotency policy')
    segments = path.split('/')
    matches = [(required, strategy) for expected_method, expected_segments, required, strategy in _POLICIES
               if expected_method == method.strip().upper() and len(expected_segments) == len(segments)
               and all((actual.startswith(expected[0]) and actual.endswith(expected[1]) and len(actual) > len(expected[0]) + len(expected[1]))
                       if isinstance(expected, tuple) else expected == actual
                       for expected, actual in zip(expected_segments, segments))]
    if len(matches) > 1:
        raise ValueError('Ambiguous client operation policy')
    if not matches:
        if explicit_strategy is None:
            raise ValueError('Unknown mutation operation requires an explicit idempotency key or policy')
        return explicit_strategy
    required, strategy = matches[0]
    if explicit_strategy is not None and explicit_strategy != strategy:
        raise ValueError('Explicit idempotency policy conflicts with the operation contract')
    return strategy if required else None
