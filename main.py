import asyncio
import time

from retry import retry


@retry(Exception, tries=3, delay=1)
def sleep_task():
    time.sleep(3)
    retry_count += 1
    if retry_count == 3:
        return 'ok'
    raise Exception('Error')


def main():
    print('Hello ...')
    sleep_task()


# Python 3.7+
main()
print('world')
