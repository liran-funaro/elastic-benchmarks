"use strict";
/*
Original author: Abram (https://gist.github.com/0x263b)

Based on code from:
https://gist.github.com/0x263b/2bdd90886c2036a1ad5bcf06d6e6fb37
*/

String.prototype.toRGB = function () {
    let hash = 0;
    if (this.length === 0) return hash;
    for (let i = 0; i < this.length; i++) {
        hash = this.charCodeAt(i) + ((hash << 5) - hash);
        hash = hash & hash;
    }
    let rgb = [0, 0, 0];
    for (let i = 0; i < 3; i++)
        rgb[i] = (hash >> (i * 8)) & 255;
    return `rgb(${rgb[0]}, ${rgb[1]}, ${rgb[2]})`;
};


String.prototype.toHue = function (m) {
    if (m === undefined)
        m = 11;
    let string_hash = md5(this);
    let hash = 0;
    if (string_hash.length === 0) return hash;
    for (let i = 0; i < string_hash.length; i++) {
        hash = string_hash.charCodeAt(i) + ((hash << 5) - hash);
        hash = hash & hash;
    }
    return (hash * m) % 360;
};
