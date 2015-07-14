// Render the markers for cab locations on Google Maps
var SF = new google.maps.LatLng(37.7577, -122.4376);
var markers = [];
var map;
var userMarker;
var updateProcess;

function initialize() {
    var mapOptions = {
        zoom: 13,
        center: SF
    };
    map = new google.maps.Map(document.getElementById('map-canvas'),
        mapOptions);
}

function update_values(position) {
    $.getJSON('/map/544909/26330',
        function (data) {
            recs = data.recs
            console.log(recs)
            clearMarkers();
            for (var i = 0; i < recs.length; i = i + 1) {
                addMarker(new google.maps.LatLng(recs[i].lat, recs[i].lng));
            }
        });
}

update_values();
function drop(lat, lng) {
    point = new google.maps.LatLng(lat, lng);
    clearMarkers();
    addMarker(point);
}

function addMarker(position) {
    markers.push(new google.maps.Marker({
        position: position,
        //icon: 'templates/images/taxi.png',
        map: map,
    }));
}

function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
        markers[i].setMap(null);
    }
    markers = [];
}

function clearUserMarker() {
    if (userMarker != null) {
        userMarker.setMap(null);
    }
}

google.maps.event.addDomListener(window, 'load', initialize);
