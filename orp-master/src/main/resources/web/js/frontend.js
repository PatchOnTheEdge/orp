/**
 * Created by Patch on 22.09.2015.
 */

var itemRepo = {};
var rankingRepo = [];

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
        itemRepo[itemId] = item;
    }
}
function parseMPRanking(responseText){
    //console.log(responseText);
    var rankings = JSON.parse(responseText).rankings;

    for(p in rankings){
        var publisher = rankings[p].publisherId;
        var ranking = rankings[p].ranking;

        var i = 1;
        for(r in ranking){
            if(i == 3){break;}
            var key = ranking[r].key;
            var rank = ranking[r].rank;
            //console.log("Item Rank = " + publisher, key, rank);
            setContent(i, publisher, itemRepo[key], rank);
            i++;

        }
    }
}
function setContent(i, publisher, item, rank){
    setTitle(i, publisher, item.title);
    setText(i, publisher, item.text);
    setArticleUrl(i, publisher, item.articleUrl);
    setImgUrl(i, publisher, item.imgUrl);
    console.log(item.title);
}
function setTitle(i, publisher, titel){
    document.getElementById("h" + "-" + publisher + "-" + i ).innerHTML = titel;
}
function setText(i, publisher, text){
    document.getElementById("t" + "-"  + publisher + "-" + i ).innerHTML = text;
}
function setArticleUrl(i, publisher, url){
    document.getElementById("l" + "-"  + publisher + "-" + i ).href = url;
}
function setImgUrl(i, publisher, url){
    document.getElementById("i" + "-"  + publisher + "-" + i).src = url;
}
httpGetAsync("/articles", parseJsonItem);
httpGetAsync("/ranking-mp", parseMPRanking);


