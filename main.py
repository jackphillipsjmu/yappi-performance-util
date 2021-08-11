import foo
from performance_metrics_util import PerformanceRunner

if __name__ == '__main__':
    html_output_path = './output/result.html'
    runner = PerformanceRunner(clock_type='CPU', profile_threads=True, html_output_path=html_output_path)

    with runner:
        foo.example_one()
        foo.sleep_for_duration(0.5, 'Sleep for Half Second')
        foo.child_call_function()
        foo.multiple_call_example()
        foo.multiple_call_example()

    # html_string = runner.generate_html_report(save_output=True)
    html_string = runner.generate_html_report(save_output=False)
    print(html_string)