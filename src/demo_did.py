from typing import Any, AsyncGenerator, Dict
from servicex_did_finder_lib import start_did_finder
import logging
from urllib.request import urlopen
import re

__log = logging.getLogger(__name__)

def root_file_extractor(url):
    page = urlopen(url) 
    #Read and decode the page from byts to string
    decoded_page = page.read().decode("utf-8") 
    pattern ="=\"(.+?)\">out" #the pattern of the string we want: link ending with .root
    match_results = re.findall(pattern, decoded_page)
    complete_root_file = [url + match_result for match_result in match_results]
    return complete_root_file

async def find_files(did_name: str, info: Dict[str, Any]) -> AsyncGenerator[Dict[str, Any], None]:
    '''For each incoming did name, generate a list of files that ServiceX can process

    Args:
        did_name (str): Dataset name
        info (Dict[str, Any]): Property bag containing `request-id`, etc. for the parent
                               request.

    Returns:
        AsyncGenerator[Dict[str, any], None]: yield each file
    '''
    root_file = root_file_extractor(did_name) # Finding files

    __log.info('Looking up dataset {did_name}',
                      extra={'requestId': info['request-id']})

    if did_name != 'dataset1':
        raise Exception(f'Dataset "{did_name}" is not known!')

    for file in root_file:
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
