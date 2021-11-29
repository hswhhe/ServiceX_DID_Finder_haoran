from typing import Any, AsyncGenerator, Dict
from servicex_did_finder_lib import start_did_finder
import logging

__log = logging.getLogger(__name__)

async def find_files(did_name: str, info: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
    '''For each incoming did name, generate a list of files that ServiceX can process

    Args:
        did_name (str): Dataset name
        info (Dict[str, Any]): Property bag containing `request-id`, etc. for the parent
                               request.

    Returns:
        AsyncGenerator[Dict[str, any], None]: yield each file
    '''
    __log.info('Looking up dataset {did_name}',
                      extra={'requestId': info['request-id']})

    if did_name != 'dataset1':
        raise Exception(f'Dataset "{did_name}" is not known!')

    yield {
        'file_path': "http://opendata.cern.ch/record/3827/files/mc_167740.WenuWithB.root",  # Path accessible via transformers (root, http)
        'adler32': 0,  # No clue
        'file_size': 0,  # Size in bytes if known
        'file_events': 0,  # Number of events if known
    }

def run_demo():
    log = logging.getLogger(__name__)

    try:
        log.info('Starting demo DID finder')
        start_did_finder('demo', find_files)
    finally:
        log.info('Done running demo DID finder')

if __name__ == "__main__":
    run_demo()
