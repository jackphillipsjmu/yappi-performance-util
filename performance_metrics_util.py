import os

import yappi
from pathlib import Path
import collections
import pandas as pd
from bs4 import BeautifulSoup
import foo

# Constants
DEFAULT_CSS_FILE = './resource/style.css'
DEFAULT_SCRIPT_FILE = './resource/script.js'

IGNORE_NAMES = {
    'PerformanceRunner.__exit__'
}

YAPPI_STAT_MAP = {
    'name': 0,
    'module': 1,
    'lineno': 2,
    'ncall': 3,
    'nactualcall': 4,
    'builtin': 5,
    'ttot': 6,
    'tsub': 7,
    'index': 8,
    'children': 9,
    'ctx_id': 10,
    'ctx_name': 11,
    'tag': 12,
    'tavg': 14,
    'full_name': 15
}

YAPPI_CHILD_STAT_MAP = {
    'name': 0,
    'module': 1,
    'lineno': 2,
    'ncall': 3,
    'nactualcall': 4,
    'builtin': 5,
    'ttot': 6,
    'tsub': 7,
    'index': 8,
    'children': 9,
    'ctx_id': 10,
    'ctx_name': 11,
    'tag': 12,
    'tavg': 14,
    'full_name': 15
}

RENAME_PARENT_METRICS_MAP = {
    'index': 'ID',
    'name': 'Name',
    'ncall': 'Total Calls',
    'ttot': 'Total Time',
    'tsub': 'Total Time (Excluding Subcalls)',
    'tavg': 'Average Call Time',
    'children': 'Child Call Count'
}

RENAME_OVERVIEW_METRICS_MAP = {
    'name': 'Metric',
    'min': 'Minimum',
    'median': 'Median',
    'max': 'Max',
    'total': 'Overall Total'
}

RENAME_CHILD_METRICS_MAP = {
    'parent_id': 'Parent ID',
    'parent_name': 'Parent Name',
    'name': 'Name',
    'nactualcall': 'Total Calls',
    'ttot': 'Total Time',
    'tsub': 'Total Time (Excluding Subcalls)',
    'tavg': 'Average Call Time'
}


class PerformanceRunner:

    def __init__(self,
                 clock_type: str = 'CPU',
                 builtins: bool = False,
                 profile_threads: bool = True,
                 profile_greenlets: bool = True,
                 run_name: str = 'TESTING-PERFORMANCE',
                 css_file: str = DEFAULT_CSS_FILE,
                 script_file: str = DEFAULT_SCRIPT_FILE,
                 html_output_path: str = None,
                 ignore_names: set[str] = IGNORE_NAMES):
        # The default clock is set to CPU, but you can switch to WALL clock
        self.clock_type = clock_type
        self.builtins = builtins
        self.profile_threads = profile_threads
        self.profile_greenlets = profile_greenlets

        self.run_name = run_name
        self.func_stats = None
        self.path_to_save = None
        self.output_type = None
        self.css_file = css_file
        self.script_file = script_file
        self.ignore_names = ignore_names
        self.html_output_path = html_output_path

        yappi.set_clock_type(self.clock_type)

    def __enter__(self):
        yappi.start(
            builtins=self.builtins,
            profile_threads=self.profile_threads,
            profile_greenlets=self.profile_greenlets
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.func_stats: yappi.YFuncStats = yappi.get_func_stats()

    def get_stats(self) -> yappi.YFuncStats:
        """
        Return Statistic Metrics from Performance run.

        Stat Metric Definitions
        - ncall is the number of calls
        - tsub is the total time spent in the function excluding the subcalls
        - ttot is the total time including them
        - tavg is the per call time (ttot divided by ncall)
        """
        return self.func_stats

    def _metrics_legend(self):
        return pd.DataFrame.from_dict(self._metrics_legend_dict())

    def _metrics_legend_dict(self):
        return {
            'Column': ['ID', 'Parent ID', 'Name', 'Total Calls', 'Total Time', 'Total Time (Excluding Subcalls)',
                       'Average Call Time', 'Child Call Count'],
            'YAPPI Column': ['index (parent)', 'index (child)', 'name', 'ncall', 'ttot', 'tsub', 'tavg', 'children'],
            'Description': [
                'Identifier for the Method/Function',
                'Parent Identifier for a Given Child Function/Method Call for Traceability',
                'Name of the Method or Function',
                'Total number of calls',
                'Total time including subcalls',
                'Total time spent in the function excluding the subcalls',
                'Per call time (ttot divided by ncall)',
                'Number of nested calls, i.e., calls to other methods or functions made from the invoked method '
                'or function. '
            ]
        }

    def _parent_performance_metrics_dict(self):
        result = collections.defaultdict(list)
        child_result = collections.defaultdict(list)
        stats = self.get_stats()
        for stat in stats:

            if self.set_contains(IGNORE_NAMES, stat.name):
                continue

            # Get index and name for stat for use later when processing child data
            stat_index = stat.get(8)
            stat_name = stat.get(0)
            for key, value in YAPPI_STAT_MAP.items():
                stat_value = stat.get(value)
                if key == 'children':
                    if len(stat_value) > 0:
                        for child_stat in stat.children:
                            child_dict_to_merge = self.children_to_dict(child_stat, stat_index, stat_name)
                            child_result = self.merge_defaultdicts(child_result, child_dict_to_merge)
                    stat_value = len(stat_value)
                result[key].append(stat_value)

        return result, child_result

    def _overview_from_parent_metrics_dict(self, parent_performance_metrics):
        overview_columns = ['ncall', 'ttot', 'tsub', 'tavg', 'children']
        overview_results = {
            'name': [],
            'min': [],
            'median': [],
            'max': [],
            'total': []
        }

        for col in overview_columns:
            min_val = parent_performance_metrics[col].min()
            median_val = parent_performance_metrics[col].median()
            max_val = parent_performance_metrics[col].max()
            total_val = parent_performance_metrics[col].sum()

            overview_results['name'].append(RENAME_PARENT_METRICS_MAP[col])
            overview_results['min'].append(min_val)
            overview_results['median'].append(median_val)
            overview_results['max'].append(max_val)
            overview_results['total'].append(total_val)
        return overview_results

    def generate_html_report(self, override_html_output_path: str = None, save_output: bool = True):
        overview_table_id = 'overview_table'
        per_function_table_id = 'parent_perf_table'
        child_table_id = 'child_table'
        legend_table_id = 'legend_table'

        if override_html_output_path is not None:
            self.html_output_path = override_html_output_path

        self.ensure_dir(self.html_output_path)
        parent_metrics_dict, child_metrics_dict = self._parent_performance_metrics_dict()
        parent_metrics_df = pd.DataFrame.from_dict(parent_metrics_dict)
        child_metrics_df = pd.DataFrame.from_dict(child_metrics_dict)

        overview_results_dict = self._overview_from_parent_metrics_dict(parent_performance_metrics=parent_metrics_df)
        metrics_legend_dict = self._metrics_legend_dict()

        overview_metrics_df = pd.DataFrame.from_dict(overview_results_dict)
        metrics_legend_df = pd.DataFrame.from_dict(metrics_legend_dict)

        overview_table_html = self.create_html_table_from_df(
            overview_metrics_df,
            index=False,
            table_id=overview_table_id,
            classes='table table-striped',
            columns=['name', 'min', 'median', 'max', 'total'],
            table_header_str='Overall Performance Metrics',
            rename_header_map=RENAME_OVERVIEW_METRICS_MAP
        )

        per_function_table_html = self.create_html_table_from_df(
            parent_metrics_df,
            index=False,
            table_id=per_function_table_id,
            classes='table table-striped',
            columns=['index', 'name', 'ncall', 'ttot', 'tsub', 'tavg', 'children'],
            table_header_str='Parent Performance Metrics',
            rename_header_map=RENAME_PARENT_METRICS_MAP
        )

        child_table_html = self.create_html_table_from_df(
            child_metrics_df,
            index=False,
            table_id=child_table_id,
            classes='table table-striped',
            columns=['parent_id', 'parent_name', 'name', 'nactualcall', 'ttot', 'tsub', 'tavg'],
            table_header_str='Child Performance Metrics',
            rename_header_map=RENAME_CHILD_METRICS_MAP
        )

        legend_table_html = self.create_html_table_from_df(metrics_legend_df,
                                                           table_id=legend_table_id,
                                                           index=False,
                                                           classes='table table-striped',
                                                           table_header_str='Performance Legend')

        body = self._build_table_html_body([
            overview_table_html, per_function_table_html, child_table_html, legend_table_html
        ])
        css = self.read_file(self.css_file)
        script = self.read_file(self.script_file)
        with_end = self.fill_html_document(css, body, script)
        soup = BeautifulSoup(with_end, 'html.parser')
        pretty_html = soup.prettify()

        if save_output:
            self.save_to_file(self.html_output_path, pretty_html)

        return pretty_html

    def save_to_file(self, file_path, data):
        print(f'Writing to {file_path}')
        text_file = open(file_path, "wt")
        text_file.write(data)
        text_file.close()

    def create_html_table_from_df(self,
                                  df,
                                  table_id: str,
                                  index: bool = False,
                                  columns=None,
                                  classes=None,
                                  sortable: bool = True,
                                  table_header_str: str = None,
                                  rename_header_map: dict = {},
                                  table_header_style_map=None):

        if table_header_style_map is None:
            table_header_style_map = {}
        # If we have no data but have columns then create a new empty dataframe
        if df.empty:
            df = pd.DataFrame(columns=columns) if columns is not None else pd.DataFrame(columns=['Empty Table'])

        main_html_table = df.to_html(index=index,
                                     table_id=table_id,
                                     classes=classes,
                                     columns=columns)

        table_id = table_id if table_id is not None else 'sortable-table-placeholder'
        soup = BeautifulSoup(main_html_table, 'html.parser')
        th_count = 0

        dict_attributes = {
            "class": "tooltip"
        }

        style_header_str = ''
        for key, value in table_header_style_map.items():
            style_header_str += f'{key}: {value};'

        if len(table_header_style_map) > 0:
            dict_attributes['style'] = style_header_str

        sort_svg = None

        show_hide_row_span = """  
            <span id="{table_id}_toggle" onclick="toggleTable('{table_id}')">
                <i class="fa fa-plus-circle" aria-hidden="true"></i>
            </span> 
        """.format(table_id=table_id)
        show_hide_tag = BeautifulSoup(show_hide_row_span, 'html.parser')

        for table_header in soup.find_all('th'):
            if sortable:
                dict_attributes['onclick'] = f'sortTable({th_count}, \'{table_id}\')'
                sort_svg = BeautifulSoup(self._svg_sort_icons(), 'html.parser')

            new_tag = soup.new_tag("th", attrs=dict_attributes)
            header_string = table_header.string.extract()
            header_string = header_string if header_string not in rename_header_map.keys() else rename_header_map[
                header_string]
            new_tag.string = header_string

            span_tag = soup.new_tag("span", attrs={"class": "tooltiptext"})
            span_tag.string = f'Click to Sort'

            if sort_svg is not None:
                new_tag.insert(2, sort_svg)

            table_header.replace_with(new_tag)
            th_count += 1
        table_by_id = soup.find(id=table_id)
        hr_tag = soup.new_tag('hr')

        if table_header_str is not None:
            header_tag = soup.new_tag('h2')
            header_tag.string = f'{table_header_str}'
            header_tag.insert(2, show_hide_tag)
            table_by_id.insert_before(header_tag)
            table_by_id['style'] = 'display: none;'

        table_by_id.insert_after(hr_tag)

        return soup.prettify()

    def children_to_dict(self, child: yappi.YChildFuncStats, parent_id: int = None, parent_name: str = None):
        # if not child.empty() and child._as_list
        child_result = collections.defaultdict(list)

        child_result['index'].append(child.index)
        child_result['parent_id'].append(parent_id)
        child_result['parent_name'].append(parent_name)
        child_result['name'].append(child.name)
        child_result['full_name'].append(child.full_name)
        child_result['nactualcall'].append(child.nactualcall)
        child_result['ttot'].append(child.ttot)
        child_result['tsub'].append(child.tsub)
        child_result['tavg'].append(child.tavg)

        return child_result

    def merge_defaultdicts(self, d, d1, values_are_list=True):
        for k, v in d1.items():
            if k in d:
                if values_are_list:
                    d[k].extend(d1[k])
                else:
                    d[k].update(d1[k])
            else:
                d[k] = d1[k]
        return d

    def create_html_styling_string(self):
        return self.read_file('/Users/jphillips/dev/azure_playground/AzureFunctionSamples/tests/style.css')

    def fill_html_document(self, css: str, body: str, script: str):
        return """
        <!DOCTYPE html>
        <html>
            <head>
                <title>Performance Metrics</title>
                <style>
                    {css_style}
                </style>
                <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
            </head>
            <body>
                <h1 style="text-align:center">Python Performance Metrics</h1>
                <p id='toggle_tables' style="text-align:center"><a href="#" onclick="toggleAllTables()">Show All</a></p>
                {body}
                <script>
                    {script}
                </script>
                <p style="text-align:center; opacity:0.5;"><i>Generated using <a href="https://pypi.org/project/yappi/">YAPPI</a> and Custom Code from Jack Phillips</i></p>
            </body>
        </html>
        """.format(css_style=css, body=body, script=script)

    def _build_table_html_body(self, tables: list[str]):
        return ' '.join(tables)

    def _refresh_table_html(self, table_id):
        return """
          <a onclick="resetTable('{table_id}')">
             <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise" viewBox="0 0 16 16">
              <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/>
              <path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/>
            </svg>
          </a>
        """.format(table_id=table_id)

    def _svg_sort_icons(self):
        return """
              <!-- Filter -->
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-filter" viewBox="0 0 16 16">
                <path d="M6 10.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 0 1h-3a.5.5 0 0 1-.5-.5zm-2-3a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zm-2-3a.5.5 0 0 1 .5-.5h11a.5.5 0 0 1 0 1h-11a.5.5 0 0 1-.5-.5z"/>
              </svg>
              <!-- Down Arrow (Asc) -->
              <svg style="display: none" xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-sort-down" viewBox="0 0 16 16">
               <path d="M3.5 2.5a.5.5 0 0 0-1 0v8.793l-1.146-1.147a.5.5 0 0 0-.708.708l2 1.999.007.007a.497.497 0 0 0 .7-.006l2-2a.5.5 0 0 0-.707-.708L3.5 11.293V2.5zm3.5 1a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zM7.5 6a.5.5 0 0 0 0 1h5a.5.5 0 0 0 0-1h-5zm0 3a.5.5 0 0 0 0 1h3a.5.5 0 0 0 0-1h-3zm0 3a.5.5 0 0 0 0 1h1a.5.5 0 0 0 0-1h-1z"/>
              </svg>
              <!-- Up Arrow (Desc) -->
              <svg style="display: none" xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-sort-up" viewBox="0 0 16 16">
               <path d="M3.5 12.5a.5.5 0 0 1-1 0V3.707L1.354 4.854a.5.5 0 1 1-.708-.708l2-1.999.007-.007a.498.498 0 0 1 .7.006l2 2a.5.5 0 1 1-.707.708L3.5 3.707V12.5zm3.5-9a.5.5 0 0 1 .5-.5h7a.5.5 0 0 1 0 1h-7a.5.5 0 0 1-.5-.5zM7.5 6a.5.5 0 0 0 0 1h5a.5.5 0 0 0 0-1h-5zm0 3a.5.5 0 0 0 0 1h3a.5.5 0 0 0 0-1h-3zm0 3a.5.5 0 0 0 0 1h1a.5.5 0 0 0 0-1h-1z"/>
             </svg>
        """

    def read_file(self, file_path: str):
        if os.path.exists(file_path):
            return Path(file_path).read_text()
        raise ValueError(f'Cannot read unknown file {file_path}')

    def contains(self, string: str, substring: str):
        return substring.lower() in string.lower()

    def set_contains(self, collection, string: str):
        for substring in collection:
            if self.contains(string, f'{substring}'):
                return True
        return False

    def ensure_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            print(f'Creating Directory {file_path}')
            os.makedirs(directory)
        else:
            print(f'{file_path} Exists')
