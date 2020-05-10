"use strict";
/*
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

const urlRegex = /(^|\s|'|"|,)([a-z\d-_+.]+)?((?:\/[a-z\d-_+.]+)+)($|\s|'|"|,)/igm;

const name_colors = {
    info: 95,
    warning: 60,
    error: 0,
    critical: 0,
};

let globalPath = "";
let globalLogsData = null;
let poll = false;
let lastScroll = 0;

let main_counter = 0;
let info_counter = 0;
let warning_counter = 0;
let error_counter = 0;


$(window).ready(function () {
    // Maybe one day: http://w2ui.com/web/demo/grid
    // http://w2ui.com/web/demos/#!grid/grid-9
    DEFAULT_ERROR_HANDLER = onAsyncError;

    let path_input = document.getElementById("path");
    // Execute a function when the user releases a key on the keyboard
    path_input.addEventListener("keyup", function (e) {
        adjustInputSize(path_input);
        if (e.key === 'Enter') {
            // Cancel the default action, if needed
            e.preventDefault();
            onAddressChange($(path_input).val());
        } else if (e.key === 'Escape') {
            e.preventDefault();
            $(path_input).val(globalPath);
        }
    });

    let filter_input = document.getElementById("filter");
    // Execute a function when the user releases a key on the keyboard
    filter_input.addEventListener("keyup", function () {
        adjustInputSize(filter_input);
        onFilter($(filter_input).val());
    });

    onResize();
    onHashChange();

    asyncGetTree(function(data) {
        let tree = data["tree"];
        autocomplete(path_input, tree);
    });
});


function applyFilter(tr) {
    let filter_input = document.getElementById("filter");
    onFilter($(filter_input).val(), tr);
}


function adjustInputSize(input_elem) {
    let w = input_elem.value.length + 1;
    if (w < 28)
        w = 28;
    input_elem.style.width = `${w}ch`;
}


function updateCounters() {
    $("#counter").html(main_counter);
    $("#info_counter").html(info_counter);
    $("#warning_counter").html(warning_counter);
    $("#error_counter").html(error_counter);
}


function restartListen(path, wait_for_start) {
    globalPath = path;
    poll = true;
    globalLogsData = null;
    lastScroll = 0;

    setAddress(path);

    main_counter = 0;
    info_counter = 0;
    warning_counter = 0;
    error_counter = 0;
    updateCounters();
    $("#counter").removeClass('suspend').removeClass('dead');

    $("#errors").html("");
    $("#headers").html("");
    $("#log_content").html("");

    if (wait_for_start === undefined)
        wait_for_start = false;
    asyncGetLog({path: path, wait_for_start: wait_for_start}, gotLogs);
}


function getColumnClassName(colName) {
    colName = colName.replace(":", "_");
    return `col_${colName}`;
}

function getClassStyle(className) {
    return $(`#style_${className}`);
}

function showHideColumn(colName) {
    let className = getColumnClassName(colName);
    let style = getClassStyle(className);
    if (style.length > 0) {
        showColumn(colName);
    } else {
        hideColumn(colName);
    }
}

function hideColumn(colName, verify) {
    let className = getColumnClassName(colName);
    if (verify === true) {
        let style = getClassStyle(className);
        if (style.length > 0)
            return;
    }

    // language=HTML
    let style = $(`<style id="style_${className}">
        .${className} {
                max-width: 10px !important;
                white-space: nowrap !important;
                overflow: hidden;
                text-overflow: ellipsis;
         }
    </style>`);
    $('html > head').append(style);
    reAdjustHeaders();
}

function showColumn(colName) {
    let className = getColumnClassName(colName);
    let style = getClassStyle(className);
    style.remove();
    reAdjustHeaders();
}

function hideClass(className) {
    if (getClassStyle(className).length > 0)
        return;
    let style = $(`<style id="style_${className}">.${className} { display: none; } </style>`);
    $('html > head').append(style);
}

function showClass(className) {
    getClassStyle(className).remove();
}


function setHeaders(headers_data) {
    let row = document.getElementById("headers");
    if (row.innerHTML !== "")
        return;

    $(headers_data).each(function (i, e) {
        let th = document.createElement('th');
        th.className = getColumnClassName(e);
        th.onclick = function () {
            let colName = $(this).text();
            showHideColumn(colName);
        };
        th.innerHTML = e;
        row.appendChild(th);
    });

    // Hide process by default...
    hideColumn('process', true);

    updateLogHeight();
}


function linkify(text) {
    return text.replace(urlRegex, function (match, g1, g2, g3, g4) {
        let link_str = g3;
        let link = g3;
        if (g2 !== undefined) {
            link_str = g2 + g3;
            link = g2 + g3;
        }
        if (g2 === '.') {
            let h = window.location.hash;
            if (h.length <= 1)
                return;

            link = h.substr(1) + g3;
        }
        return `${g1}<a href='#${link}' class='path-link'>${link_str}</a>${g4}`;
    });
}


//#########################################################################################
//# Row
//#########################################################################################
function insertRow(row_data, headers_data, showUpdateMarker) {
    // Find a <table> element with id="log":
    let table = document.getElementById("log_content");

    // Create an empty <tr> element and add it to the 1st position of the table:
    let row = table.insertRow();
    let $row = $(row);
    let index = row.rowIndex;
    let alternate = (index % 2 === 0);
    if (alternate)
        $row.addClass('alternateRow');

    for (let i = 0; i < row_data.length; i++) {
        let e = row_data[i];
        let h = headers_data[i];
        let cell = row.insertCell();
        cell.className = getColumnClassName(h);

        // Hide thread name if it is equal to the module
        if (h === "thread" && e === row_data[i + 1])
            continue;
        if (h === "message")
            e = linkify(e);

        cell.innerHTML = e;

        if (h === 'level') {
            let level = e.toLowerCase();
            row.classList.add(`level_${level}`);
            let hue = name_colors[level];
            if (hue !== undefined) {
                let color;
                if (alternate)
                    color = `hsl(${hue}, 70%, 50%)`;
                else
                    color = `hsl(${hue}, 70%, 40%)`;
                $(cell).css("background-color", color).css("color", 'black');
            }

            main_counter++;

            switch (level) {
                case 'info':
                    info_counter++;
                    break;
                case 'warning':
                    warning_counter++;
                    break;
                case 'error':
                case 'critical':
                    error_counter++;
                    break;
            }
        } else if (h === "module") {
            let hue = e.toHue();
            $row.css("color", `hsl(${hue}, 100%, 85%)`);
        } else if (h === "source") {
            let hue = e.toHue(3);
            let color;
            if (alternate) {
                color = `hsl(${hue}, 25%, 30%)`;
            } else {
                color = `hsl(${hue}, 25%, 20%)`;
            }
            $(cell).css("background-color", color);
        }

        if (i === 0 && showUpdateMarker === true) {
            let $cell = $(cell);
            $cell.css("background-color", "#FF0000");
            let target = "#000";
            if (alternate)
                target = "#333";
            $cell.animate({backgroundColor: target}, 5000);
        }
    }

    return row;
}


//#########################################################################################
//# Logs listening
//#########################################################################################

function gotLogs(data) {
    let path = data["url"];
    let logs_data = data["logs_data"];
    if (path !== globalPath)
        return;
    let showUpdateMarker = (globalLogsData != null);
    globalLogsData = logs_data;

    setHeaders(data["headers"]);
    iterativeLogUpdate(data, 0, showUpdateMarker);
}

function iterativeLogUpdate(data, position, showUpdateMarker) {
    let lines_per_iteration = 100;
    let log = data["joint_log"];
    let headers = data["headers"];

    let next_position = Math.min(position + lines_per_iteration, log.length);

    let rows = [];
    for (let i = position; i < next_position; i++)
        rows.push(insertRow(log[i], headers, showUpdateMarker));

    updateCounters();
    reAdjustHeaders();

    if (rows.length > 0 && $("#scroll").is(':checked'))
        rows[rows.length - 1].scrollIntoView();

    applyFilter(rows);

    if (next_position >= log.length)
        finishLogUpdate(data);
    else
        setTimeout(function () {
            iterativeLogUpdate(data, next_position, showUpdateMarker);
        }, 50);
}

function finishLogUpdate(data) {
    let path = data["url"];
    let logs_data = data["logs_data"];

    if (data["is_running"] && poll && path === globalPath) {
        $("#counter").removeClass('suspend').removeClass('dead');
        $("#listen").val('Pause');
        setTimeout(function () {
            asyncGetLog({path: path, logs_data: logs_data}, gotLogs);
        }, 100);
    } else if (poll) {
        $("#counter").removeClass('suspend').addClass('dead');
        $("#listen").val('Resume');
    } else {
        $("#counter").removeClass('dead').addClass('suspend');
        $("#listen").val('Resume');
    }
}

function suspendListening() {
    poll = false;
}

function resumeListening() {
    poll = true;
    asyncGetLog({path: globalPath, logs_data: globalLogsData}, gotLogs);
}

function stopResumeListening() {
    if (!poll || $("#counter").hasClass('dead')) {
        resumeListening();
    } else {
        suspendListening();
    }
}
