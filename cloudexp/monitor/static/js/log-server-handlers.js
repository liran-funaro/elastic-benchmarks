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


//#########################################################################################
//# Error
//#########################################################################################
function onAsyncError(message, traceback) {
    let hide_button = document.createElement("button");
    hide_button.innerHTML = "Close";
    $(hide_button).addClass('close-btn');

    hide_button.addEventListener("click", function () {
        $("#errors").html("");
        reAdjustHeadersAndHeight();
    });

    let trace = document.createElement("pre");
    trace.appendChild(document.createTextNode(traceback));

    let trace_button = document.createElement("span");
    let trace_ck = document.createElement("input");
    trace_ck.type = "checkbox";
    trace_ck.name = "trace";
    trace_ck.id = "trace";
    trace_ck.setAttribute("hidden", "true");

    trace_button.addEventListener("click", function () {
        if ($(trace_ck).prop('checked'))
            $(trace).show();
        else
            $(trace).hide();
        reAdjustHeadersAndHeight();
    });

    let trace_label = document.createElement("label");
    trace_label.setAttribute("for", trace_ck.id);
    trace_label.appendChild(document.createTextNode("Toggle Trace"));
    trace_button.appendChild(trace_ck);
    trace_button.appendChild(trace_label);

    let msg = document.createElement("pre");
    msg.append(document.createTextNode(" "));
    msg.append(hide_button);
    msg.append(document.createTextNode(" "));
    msg.append(trace_button);
    msg.append(document.createTextNode(" "));
    msg.appendChild(document.createTextNode(message));

    $("#errors").append(msg).append(trace);
    $(trace).hide();
    reAdjustHeadersAndHeight();
}


//#########################################################################################
//# Address
//#########################################################################################
function setAddress(path) {
    let path_input = document.getElementById("path");
    $(path_input).val(path);
    adjustInputSize(path_input);
    window.location.hash = path;
}

function onAddressChange(path, from_hash) {
    if (path.endsWith('.log')) {
        let i = path.lastIndexOf('/');
        path = path.substr(0, i);
    }

    if (path === globalPath)
        return setAddress(path);

    asyncGetRelativePath({path: path}, function (data) {
        let url = data["url"];
        if (path !== url) {
            if (from_hash === true)
                history.replaceState(null, null, `#${url}`);
            return onAddressChange(url, from_hash);
        }

        restartListen(path);
    });
}

function onPathUp() {
    let path_input = document.getElementById("path");
    let path = $(path_input).val();
    onAddressChange(path + '/..');
}

//#########################################################################################
//# Hash
//#########################################################################################
function onHashChange() {
    let h = window.location.hash;
    if (h.length <= 1)
        return;

    let path = h.substr(1);
    if (path !== globalPath)
        onAddressChange(path, true);
}

//#########################################################################################
//# Resize
//#########################################################################################
function updateWindowSize() {
    window.window_height = window.innerHeight;
    window.marginTop = parseInt(window.getComputedStyle(document.body).getPropertyValue('margin-top'));
    window.marginBottom = parseInt(window.getComputedStyle(document.body).getPropertyValue('margin-bottom'));
    window.maxHeight = window.window_height - window.marginTop - window.marginBottom;

    window.window_width = window.innerWidth;
    window.marginLeft = parseInt(window.getComputedStyle(document.body).getPropertyValue('margin-left'));
    window.marginRight = parseInt(window.getComputedStyle(document.body).getPropertyValue('margin-right'));
    window.maxWidth = window.window_width - window.marginLeft - window.marginRight;
}

function updateTableTopPos() {
    window.table_offset = $(".scrollContent").offset();
}

function onResize() {
    updateWindowSize();
    reAdjustHeadersAndHeight();
}

function reAdjustHeadersAndHeight() {
    updateLogHeight();
    reAdjustHeaders()
}

function updateLogHeight() {
    updateTableTopPos();
    $("#log_content").css("height", window.maxHeight - window.table_offset.top);
}

function setLastChildStyle(style_str) {
    let style = $("#style_last_child");
    if (style.length === 0) {
        style = $("<style id='style_last_child'>");
        $('html > head').append(style);
    }

    style.html(`tbody.scrollContent td:last-child {${style_str}}`);
}


// Performance overhead is too high for this
function reAdjustColWidth() {
    let minLastChildWidth = window.maxWidth * 0.5;
    let lastChildWidth = $('tbody.scrollContent td:visible:last-child:first').width();
    let tableWidth = $("#log").width();
    let targetWidth = lastChildWidth + (window.maxWidth - tableWidth);
    if (targetWidth < minLastChildWidth)
        targetWidth = minLastChildWidth;

    setLastChildStyle("width: " + targetWidth + "px;");
}


function reAdjustHeaders() {
    // Header width
    let cols = $("#log_content tr:visible:first").children("td");
    let head = $("#headers").children("th");
    for (let i = 0; i < cols.length; i++) {
        let $h = $(head[i]);
        let $c = $(cols[i]);
        let head_borderWidth = parseInt($h.css("border-left-width")) + parseInt($h.css("border-right-width"));
        let c_borderWidth = parseInt($c.css("border-left-width")) + parseInt($c.css("border-right-width"));
        let w = $c.width() + c_borderWidth - head_borderWidth;
        $h.css('width', w).css('max-width', w);
    }
}

//#########################################################################################
//# Scroll
//#########################################################################################
function onScroll() {
    let table = $("#log_content");
    let maxScroll = table.prop("scrollHeight") - table.outerHeight();
    let curScroll = table.scrollTop();
    if (curScroll < lastScroll) {
        $("#scroll").prop('checked', false);
    } else if (maxScroll === curScroll || maxScroll === 0) {
        $("#scroll").prop('checked', true);
    }
    lastScroll = curScroll;
}

function onAutoScroll() {
    let table = $("#log_content");
    let maxScroll = table.prop("scrollHeight") - table.outerHeight();
    table.scrollTop(maxScroll);
}

//#########################################################################################
//# Level
//#########################################################################################
function onChangeLevel(level) {
    lastScroll = 0;
    let all_levels = ['debug', 'info', 'warning', 'error', 'critical'];
    let show = false;
    for (let i in all_levels) {
        if (level === all_levels[i])
            show = true;

        let levelClass = 'level_' + all_levels[i];
        if (show)
            showClass(levelClass);
        else
            hideClass(levelClass);
    }

    reAdjustHeaders();
}

//#########################################################################################
//# Filter
//#########################################################################################
function onFilter(regexp_str, tr) {
    let regexp;
    try {
        regexp = new RegExp(regexp_str, 'i');
    } catch (err) {
        console.log(regexp_str, err);
        return;
    }

    lastScroll = 0;

    let $clear_filter = $("#clear-filter");
    let $tr = $(tr ? tr : "tr");
    let $td = $tr.children("td");
    if (regexp_str.length === 0) {
        $tr.removeClass('hidden');
        $td.removeClass('match');
        $clear_filter.hide();
        reAdjustHeaders();
        return;
    }

    $clear_filter.show();


    $tr.addClass('hidden');
    $td.removeClass('match');
    let to_show = $td.filter(function (i, elem) {
        let m = elem.innerHTML.match(regexp);
        if (m)
            $(elem).addClass('match');
        return m;
    });
    to_show.parent().removeClass('hidden');
    reAdjustHeaders();
}

function onClearFilter() {
    $("#filter").val("");
    onFilter("");
}

//#########################################################################################
//# Action
//#########################################################################################
function onAction(elem) {
    let action = elem.value;
    let overwrite = false;
    let sig = 'int';
    // noinspection FallThroughInSwitchStatementJS
    switch (action) {
        case 'launch-overwrite':
            overwrite = true;
        case 'launch':
            asyncLaunch({path: globalPath, overwrite: overwrite, wait_for_start: true, batch: false}, function () {
                restartListen(globalPath, true);
            });
            break;
        case 'batch-overwrite':
            overwrite = true;
        case 'batch':
            asyncLaunch({path: globalPath, overwrite: overwrite, wait_for_start: true, batch: true}, function () {
                restartListen(globalPath, true);
            });
            break;
        case 'kill':
            sig = 'kill';
        case 'terminate':
            asyncTerminate({path: globalPath, sig: sig});
            break;
    }

    $(elem).val("");
}
