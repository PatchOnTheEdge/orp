/**
 * Created by Patch on 22.09.2015.
 */

var itemRepo = {};
var rankingRepo = {};

function httpGetAsync(theUrl, callback) {
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function() {
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200) {
            callback(xmlHttp.responseText);
        }
    };
    xmlHttp.open("GET", theUrl, true); // true for asynchronous
    xmlHttp.send(null);
}

function setDiv(){

}

function parseJsonItem(responseText){
    //console.log(responseText);
    var items = JSON.parse(responseText).items;

    for(i in items){
        var itemId = items[i].itemId;
        var item = items[i].item;
        console.log(itemId);
        itemRepo[itemId] = item;
    }
}
function parseJsonRanking(responseText){
    //console.log(responseText);
    var rankings = JSON.parse(responseText).rankings;

    for(p in rankings){
        var publisher = rankings[p].publisherId;
        var ranking = rankings[p].ranking;

        for(r in ranking){
            var key = ranking[r].key;
            var rank = ranking[r].rank;
            console.log("Item Rank = " + publisher, key, rank);

        }
    }
}
httpGetAsync("/items", parseJsonItem);
httpGetAsync("/ranking-mp", parseJsonRanking);


