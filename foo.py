import time


def sleep_for_duration(seconds_to_sleep, name: str = 'sleep_for_duration'):
    print(f'Starting {name} and sleeping for {seconds_to_sleep} seconds')
    time.sleep(seconds_to_sleep)
    print(f'\tFinished with {name}')


def child_call_function():
    sleep_for_duration(0.75, 'other_function')


def example_one():
    print('Executing example_one')


def multiple_call_example():
    print('Executing multiple_call_example')


def current_milli_time():
    return round(time.time() * 1000)