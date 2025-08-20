
from datetime import datetime, timezone
from logging import Logger
from hyperliquid.utils.types import SpotAssetInfo
from hyperliquid.info import Info


DATE_FORMAT = '%Y-%m-%d %H:%M:%S UTC'


def get_current_timestamp_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def format_currency(value):
    if value >= 1_000_000_000_000:
        return f"${value/1_000_000_000_000:.2f}T"
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"


# with caching and show_spinner=false
# even if this is wrapped around a spinner,
# that spinner only runs whenever data is NOT fetched from cache
# i.e. if data is actually fetched
# hence reducing unnecessary quick spinner upon fetching from cache
def get_cached_unit_token_mappings(info: Info, logger: Logger) -> dict[str, tuple[str, int]]:
    spot_metadata = info.spot_meta()

    unit_tokens = (t for t in spot_metadata['tokens']
                    if t.get('fullName') is not None and
                    str(t['fullName']).startswith('Unit '))

    universe_metadata: list[SpotAssetInfo] = spot_metadata['universe']
    universe_metadata_idx = 0
    mapping = {}

    for t in unit_tokens:
        token_name = t['name']
        token_idx = t['index']

        try:
            # used for bridge
            token_decimals = int(t['weiDecimals'])
            if t['evmContract'] is not None:
                token_decimals += int(t['evmContract']['evm_extra_wei_decimals'])
        except Exception:
            # skip
            logger.warning(f'skipping as unable to find decimals info for {token_name}: {t}')
            continue

        universe_entry = None
        while universe_metadata_idx < len(universe_metadata):
            if token_idx in universe_metadata[universe_metadata_idx]['tokens']:
                universe_entry = universe_metadata[universe_metadata_idx]
                break
            universe_metadata_idx += 1

        if universe_entry:
            mapping[universe_entry['name']] = (token_name, token_decimals)
        else:
            logger.info(f'unable to find pair metadata for token {token_name}, skipping processing')

    return mapping