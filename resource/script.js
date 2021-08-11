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