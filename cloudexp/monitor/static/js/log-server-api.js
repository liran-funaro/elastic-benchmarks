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

// Optional error handler for the server error response
let DEFAULT_ERROR_HANDLER = function(message, traceback) {
    console.log(message);
    console.log(traceback);
};


function _errorHandler(onError) {
    if (onError === undefined)
        onError = DEFAULT_ERROR_HANDLER;

    return function (data) {
        console.log(data);
        let response = data["responseJSON"];
        return onError(response["message"], response["traceback"]);
    }
}


function asyncGetRelativePath(data, onSuccess, onError) {
    return $.ajax({
        url: "/get_relative_path",

        type: "POST",
        async: true,

        contentType: 'application/json',
        dataType: "json",

        data: JSON.stringify(data),

        success: onSuccess,
        error: _errorHandler(onError),
    });
}


function asyncGetTree(onSuccess, onError) {
    return $.ajax({
        url: "/tree",

        type: "GET",
        async: true,

        contentType: 'application/json',
        dataType: "json",

        success: onSuccess,
        error: _errorHandler(onError),
    });
}


function asyncGetLog(data, onSuccess, onError) {
    return $.ajax({
        url: "/getlogs",

        type: "POST",
        async: true,

        contentType: 'application/json',
        dataType: "json",

        data: JSON.stringify(data),

        success: onSuccess,
        error: _errorHandler(onError),
    });
}


function asyncLaunch(data, onSuccess, onError) {
    return $.ajax({
        url: "/launch",

        type: "POST",
        async: true,

        contentType: 'application/json',
        dataType: "json",

        data: JSON.stringify(data),

        success: onSuccess,
        error: _errorHandler(onError),
    });
}


function asyncTerminate(data, onSuccess, onError) {
    return $.ajax({
        url: "/terminate",

        type: "POST",
        async: true,

        contentType: 'application/json',
        dataType: "json",

        data: JSON.stringify(data),

        success: onSuccess,
        error: _errorHandler(onError),
    });
}
