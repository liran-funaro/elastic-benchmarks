"use strict";
// Taken from: https://www.w3schools.com/howto/howto_js_autocomplete.asp

function autocomplete(inp, arr) {
    /*the autocomplete function takes two arguments,
    the text field element and an array of possible autocompleted values:*/
    let currentFocus;
    /*execute a function when someone writes in the text field:*/
    inp.addEventListener("input", function() {
        let val = this.value;
        /*close any already open lists of autocompleted values*/
        closeAllLists();
        if (!val) { return false;}
        currentFocus = -1;
        /*create a DIV element that will contain the items (values):*/
        let a = document.createElement("div");
        a.setAttribute("id", this.id + "autocomplete-list");
        a.setAttribute("class", "autocomplete-items");
        /*append the DIV element as a child of the autocomplete container:*/
        this.parentNode.appendChild(a);
        /*for each item in the array...*/
        let itemCount = 0;
        for (let i = 0; i < arr.length; i++) {
            /*check if the item starts with the same letters as the text field value:*/
            if (arr[i].substr(0, val.length).toUpperCase() === val.toUpperCase()) {
                /*create a DIV element for each matching element:*/
                let b = document.createElement("div");
                /*make the matching letters bold:*/
                b.innerHTML = "<strong>" + arr[i].substr(0, val.length) + "</strong>";
                b.innerHTML += arr[i].substr(val.length);
                /*insert a input field that will hold the current array item's value:*/
                b.innerHTML += "<input type='hidden' value='" + arr[i] + "'>";
                /*execute a function when someone clicks on the item value (DIV element):*/
                b.addEventListener("click", function() {
                    /*insert the value for the autocomplete text field:*/
                    inp.value = this.getElementsByTagName("input")[0].value;
                    /*close the list of autocompleted values,
                    (or any other open lists of autocompleted values:*/
                    closeAllLists();
                    inp.dispatchEvent(new KeyboardEvent('keyup', {key: 'Enter'}));
                });
                a.appendChild(b);
                itemCount += 1;
            }
        }

        if (itemCount < 2)
            closeAllLists();
    });
    /*execute a function presses a key on the keyboard:*/
    inp.addEventListener("keydown", function(e) {
        let x = document.getElementById(this.id + "autocomplete-list");
        if (x)
            x = x.getElementsByTagName("div");

        if (!x)
            return;

        if (e.key === 'Escape') {
            /*prevent the form from being submitted*/
            // e.preventDefault();
            closeAllLists();
        } else if (e.key === 'ArrowDown') {
            /*If the arrow DOWN key is pressed, increase the currentFocus variable:*/
            currentFocus++;
            /*and and make the current item more visible:*/
            addActive(x);
        } else if (e.key === 'ArrowUp') {
            /*If the arrow UP key is pressed, decrease the currentFocus variable:*/
            currentFocus--;
            /*and and make the current item more visible:*/
            addActive(x);
        } else if (e.key === 'Enter') {
            /*prevent the form from being submitted,*/
            e.preventDefault();
            if (currentFocus > -1) {
                /*and simulate a click on the "active" item:*/
                x[currentFocus].click();
            }
        } else if (e.key === 'Tab' || e.key === 'ArrowRight') {
            /*prevent the form from being submitted*/
            e.preventDefault();

            if (currentFocus > -1)
                inp.value = x[currentFocus].getElementsByTagName("input")[0].value;
            else if (e.key === 'ArrowRight') { // take the first one
                inp.value = x[0].getElementsByTagName("input")[0].value;
            } else { // Tab: use the common string
                let minStr = x[0].getElementsByTagName("input")[0].value;
                for(let i=1; i<x.length; i++) {
                    let curStr = x[i].getElementsByTagName("input")[0].value;
                    let j;
                    for(j=0; j < Math.min(minStr.length, curStr.length); j++) {
                        if (minStr[j] !== curStr[j])
                            break;
                    }
                    minStr = minStr.substr(0, j);
                }
                inp.value = minStr;
            }

            inp.dispatchEvent(new KeyboardEvent('input', {}));
        }
    });
    function addActive(x) {
        /*a function to classify an item as "active":*/
        if (!x) return false;
        /*start by removing the "active" class on all items:*/
        removeActive(x);
        if (currentFocus >= x.length) currentFocus = 0;
        if (currentFocus < 0) currentFocus = (x.length - 1);
        /*add class "autocomplete-active":*/
        x[currentFocus].classList.add("autocomplete-active");
    }
    function removeActive(x) {
        /*a function to remove the "active" class from all autocomplete items:*/
        for (let i = 0; i < x.length; i++) {
            x[i].classList.remove("autocomplete-active");
        }
    }
    function closeAllLists(elmnt) {
        /*close all autocomplete lists in the document, except the one passed as an argument:*/
        let x = document.getElementsByClassName("autocomplete-items");
        for (let i = 0; i < x.length; i++) {
            if (elmnt !== x[i] && elmnt !== inp) {
                x[i].parentNode.removeChild(x[i]);
            }
        }
    }
    /*execute a function when someone clicks in the document:*/
    document.addEventListener("click", function (e) {
        closeAllLists(e.target);
    });
}
