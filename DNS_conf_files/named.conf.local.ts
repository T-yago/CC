// Do any local configuration here
// não esquecer que isto implica criar a pasta zones e o ficheiro CC2023.zone

zone "CC2023" {
        type master;
        file "etc/bind/zones/CC2023.zone";
};

//

// Consider adding the 1918 zones here, if they are not used in your
// organization
//include "/etc/bind/zones.rfc1918";



