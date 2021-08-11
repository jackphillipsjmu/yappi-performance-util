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


class DatabricksPerformanceRunner:

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
        css = self.css_inline()
        script = self.script_inline()
        with_end = self.fill_html_document(css, body, script)
        soup = BeautifulSoup(with_end, 'html.parser')
        pretty_html = soup.prettify()

        if save_output:
            self.save_to_file(self.html_output_path, pretty_html)

        return pretty_html

    def css_inline(self):
        return """
            body {
                 font-family: Arial, Helvetica, sans-serif;
            }
             table {
                 border-spacing: 0;
                 width: 100%;
                 border: 1px solid #ddd;
            }
             th {
                 cursor: pointer;
                 color: white;
                 background-color: rgb(58, 105, 155);
                 text-align: center;
            }
             th, td {
                 text-align: center;
                 padding: 10px;
            }
             tr:nth-child(even) {
                 background-color: rgb(220, 230, 242);
            }
             .tooltip {
                /* position: relative;
                 display: inline-block;
                 border-bottom: 1px dotted black;
                 */
            }
             .tooltip .tooltiptext {
                 visibility: hidden;
                 width: 120px;
                 background-color: black;
                 color: #fff;
                 text-align: center;
                 border-radius: 6px;
                /* padding: 5px 0;
                 */
                /* Position the tooltip */
                 position: absolute;
                 z-index: 1;
            }
             .tooltip:hover .tooltiptext {
                 visibility: visible;
            }
             .bi-arrow-clockwise {
                 transition: transform 0.2s ease-in;
            }
             .bi-arrow-clockwise:hover {
                 transform: rotate(360deg);
            }
            
            .fa-plus-circle:hover {
             opacity: 0.5;
            }
            
            .fa-minus-circle:hover {
             opacity: 0.5;
            }
        """

    def script_inline(self):
        return """
               const SHOW_ALL_TEXT = 'Show All';
               const HIDE_ALL_TEXT = 'Hide All';
               const SHOW_TABLE_HTML = '<i class="fa fa-minus-circle" aria-hidden="true"></i>';
               const HIDE_TABLE_HTML = '<i class="fa fa-plus-circle" aria-hidden="true"></i>';
            
               function isNull(value) {
                 return value === undefined || value === null;
               }
            
               function isNotNull(value) {
                 return !isNull(value);
               }
            
               function toggleAllTables() {
                // Get the HTML element that has the corresponding ID. This is used to process whether to show or hide the table.
                show_tables_elm = document.getElementById('toggle_tables');
                // Inner HTML string should have either 'Show All' or 'Hide All' which is used to determine the updated text
                // on the page.
                elm_str = show_tables_elm.innerHTML;
                // Get all HTML Table elements
                tables = document.getElementsByTagName("TABLE");
                // Process what to display, either 'Show All' or 'Hide All'
                // If the HTML text has 'Show All' set it to the inverse 'Hide All' and vice versa
                should_hide_table = elm_str.toLowerCase().includes(SHOW_ALL_TEXT.toLowerCase())
                var anchor_text = (should_hide_table ? HIDE_ALL_TEXT : SHOW_ALL_TEXT);
                // Update the Anchor HTML
                show_tables_elm.innerHTML = `<a href="#" onclick="toggleAllTables()">${anchor_text}</a>`;
                // Toggle Show/Hide Table by ID
                for (let i = 0; i < tables.length; i++) {
                 toggleTableExplicitly(tables[i].id, should_hide_table);
               }
            }
            
            function allTablesShowing() {
               // Get all HTML Table elements
               tables = document.getElementsByTagName("TABLE");
            
               for (let i = 0; i < tables.length; i++) {
                table_id = tables[i].id
                var table = document.getElementById(table_id);
                var anchor = document.getElementById(table_id + '_toggle');
                cur_style = table.style.display;
                if (cur_style === 'none') {
                 return false;
                }
               }
               return true;
            }
            
            function allTablesHidden() {
               // Get all HTML Table elements
               tables = document.getElementsByTagName("TABLE");
            
               for (let i = 0; i < tables.length; i++) {
                table_id = tables[i].id
                var table = document.getElementById(table_id);
                var anchor = document.getElementById(table_id + '_toggle');
                cur_style = table.style.display;
                if (cur_style !== 'none') {
                 return false;
                }
               }
               return true;
            }
            
            function toggleTableExplicitly(table_id, show_table) {
                var table = document.getElementById(table_id);
                cur_style = table.style.display;
                var anchor = document.getElementById(table_id + '_toggle');
            
                show_table = isNotNull(show_table) ? show_table : (cur_style === 'none');
            
                if (show_table) {
                    table.style.display = '';
                    anchor.innerHTML = SHOW_TABLE_HTML;
                } else {
                    table.style.display = 'none';
                    anchor.innerHTML = HIDE_TABLE_HTML
                }
            
                show_tables_elm = document.getElementById('toggle_tables');
                if (allTablesShowing()) {
                 show_tables_elm.innerHTML = `<a href="#" onclick="toggleAllTables()">Hide All</a>`;
                } else if (allTablesHidden()) {
                 show_tables_elm.innerHTML = `<a href="#" onclick="toggleAllTables()">Show All</a>`;
                }
            }
            
            function toggleTable(table_id) {
              toggleTableExplicitly(table_id, null);
            }
            
            function resetTable(table_id) {
                var table, rows = 0;
                table = document.getElementById(table_id);
                rows = table.rows;
                cell_count = rows[0].cells.length;
            
                for (let cell = 0; cell < cell_count + 1; cell++) {
            
                    disable_headers = rows[0].getElementsByTagName("TH")
            
                    for (let disable_i = 0; disable_i < disable_headers.length; disable_i++) {
                        disable_header = rows[0].getElementsByTagName("TH")[disable_i]
                        filter_icon = disable_header.getElementsByClassName(filter_class);
                        up_arrow_icon = disable_header.getElementsByClassName(up_arrow_class);
                        down_arrow_icon = disable_header.getElementsByClassName(down_arrow_class);
                        if (filter_icon[0] !== undefined) {
                            filter_icon[0].style.display = "initial";
                            up_arrow_icon[0].style.display = "none";
                            down_arrow_icon[0].style.display = "none";
                        }
                    }
                }
            }
            
            function sortTable(n, table_id) {
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById(table_id);
                switching = true;
                // Set the sorting direction to ascending:
                dir = "asc";
                header = null;
                headers_to_reset = []
            
                filter_class = "bi bi-filter";
                up_arrow_class = "bi bi-sort-up";
                down_arrow_class = "bi bi-sort-down";
            
                /*Make a loop that will continue until
                no switching has been done:*/
                while (switching) {
                    //start by saying: no switching is done:
                    switching = false;
                    rows = table.rows;
            
                    if (header === null) {
                        header = rows[0].getElementsByTagName("TH")[n];
                        cell_count = rows[0].cells.length;
                        // Loop through the cells/columns and mark them to reset sortable header icon
                        for (let cell = 0; cell < cell_count + 1; cell++) {
                            if (cell !== n) {
                                headers_to_reset.push(cell);
                            }
                        }
                    }
                    /*Loop through all table rows (except the
                    first, which contains table headers):*/
                    for (i = 1; i < (rows.length - 1); i++) {
                        //start by saying there should be no switching:
                        shouldSwitch = false;
                        /*Get the two elements you want to compare,
                        one from current row and one from the next:*/
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        /*check if the two rows should switch place,
                        based on the direction, asc or desc:*/
                        if (dir == "asc") {
                            if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                                //if so, mark as a switch and break the loop:
                                shouldSwitch = true;
                                break;
                            }
                        } else if (dir == "desc") {
                            if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {
                                //if so, mark as a switch and break the loop:
                                shouldSwitch = true;
                                break;
                            }
                        }
                    }
                    if (shouldSwitch) {
                        /*If a switch has been marked, make the switch
                        and mark that a switch has been done:*/
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        //Each time a switch is done, increase this count by 1:
                        switchcount++;
                    } else {
                        /*If no switching has been done AND the direction is "asc",
                        set the direction to "desc" and run the while loop again.*/
                        if (switchcount == 0 && dir == "asc") {
                            dir = "desc";
                            switching = true;
                        }
                    }
                }
            
                icon = null
                disable_icons = []
            
                cur_asc_icon = header.getElementsByClassName(up_arrow_class);
                cur_desc_icon = header.getElementsByClassName(down_arrow_class);
                cur_filter_icon = header.getElementsByClassName(filter_class);
            
                order_for_sort = [
                    cur_filter_icon[0].style.display === 'initial',
                    cur_asc_icon[0].style.display === 'initial',
                    cur_desc_icon[0].style.display === 'initial'
                ];
            
                const first_true_idx = (element) => element;
                sort_idx = order_for_sort.findIndex(first_true_idx);
            
                if (sort_idx === -1 || sort_idx === 0) {
                    disable_icons.push(cur_filter_icon, cur_desc_icon);
                    icon = cur_asc_icon;
                } else if (sort_idx === 1) {
                    icon = cur_desc_icon;
                    disable_icons.push(cur_filter_icon, cur_asc_icon);
                } else if (sort_idx === 2) {
                    icon = filter_icon;
                    disable_icons.push(cur_asc_icon, cur_desc_icon);
                }
            
                // Set filter back to default for unselected columns
                for (let idx = 0; idx < headers_to_reset.length; idx++) {
                    disable_header = rows[0].getElementsByTagName("TH")[idx]
                    filter_icon = disable_header.getElementsByClassName(filter_class);
                    if (filter_icon[0] !== undefined) {
                        filter_icon[0].style.display = "initial";
                    }
                    disable_icons.push(disable_header.getElementsByClassName(up_arrow_class), disable_header.getElementsByClassName(down_arrow_class));
                }
            
                // Disable icons that need to be hidden
                for (let idx = 0; idx < disable_icons.length; idx++) {
                    if (disable_icons[idx][0] !== undefined) {
                        disable_icons[idx][0].style.display = "none";
                    }
                }
            
                // Enable sort icon for display
                if (icon[0] !== undefined) {
                    icon[0].style.display = "initial";
                }
            }
        """

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


if __name__ == '__main__':
    html_output_path = './output/result.html'
    runner = DatabricksPerformanceRunner(clock_type='CPU', profile_threads=True, html_output_path=html_output_path)

    with runner:
        foo.example_one()
        foo.sleep_for_duration(0.5, 'Sleep for Half Second')
        foo.child_call_function()
        foo.multiple_call_example()
        foo.multiple_call_example()

    # html_string = runner.generate_html_report(save_output=True)
    html_string = runner.generate_html_report(save_output=True)
    print(html_string)