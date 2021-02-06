import sys
import time


def status_poller(intro, done_status, func):
    """
    Polls a function for status, sleeping for 10 seconds between each query,
    until the specified status is returned.

    :param intro: An introductory sentence that informs the reader what we're
                  waiting for.
    :param done_status: The status we're waiting for. This function polls the status
                        function until it returns the specified status.
    :param func: The function to poll for status. This function must eventually
                 return the expected done_status or polling will continue indefinitely.
    """
    status = None
    print(intro)
    print("Current status: ", end='')
    while status != done_status:
        prev_status = status
        status = func()
        if prev_status == status:
            print('.', end='')
        else:
            print(status, end='')
        sys.stdout.flush()
        time.sleep(10)
    print()
