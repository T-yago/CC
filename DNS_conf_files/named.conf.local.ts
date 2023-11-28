// não esquecer que isto está na pasta zones

zone "CC2023" {
        type master;
        file "etc/bind/zones/CC2023.zone";
};

// Do any local configuration here
//

// Consider adding the 1918 zones here, if they are not used in your
// organization
//include "/etc/bind/zones.rfc1918";



