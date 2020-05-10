#!/usr/bin/env perl
my $i = shift;
my $hex_i = sprintf("%02x", $i);
open(IN, "<vm-template.xml");
my $line;
while ($line = <IN>) {
    $line = ~s/\%d/$i/;
    $line = ~s/\%02x/$hex_i/;
    print $line;
}
