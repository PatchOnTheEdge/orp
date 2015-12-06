/**
 * Created by Patch on 22.09.2015.
 */

var itemRepo = {};
var publisherRepo = {};

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


function parseJsonItem(responseText){
    itemRepo = {};
    var items = JSON.parse(responseText).items;

    for(i in items){
        var itemId = items[i].itemId;
        var item = items[i].item;
        itemRepo[itemId] = item;
    }
}
function parseMPRanking(responseText){
    publisherRepo = {};
    var rankings = JSON.parse(responseText).rankings;

    for(p in rankings){
        var publisher = rankings[p].publisherId;
        var ranking = rankings[p].ranking;

        var i = 1;
        for(r in ranking){
            //if(i == 3){break;}
            var key = ranking[r].key;
            var rank = ranking[r].rank;

            var articles = {};
            var article = {};
            if(key in itemRepo){
                article.rank = rank;
                article.title = itemRepo[key].title;
                article.text = itemRepo[key].text;
                article.url = itemRepo[key].articleUrl;
                article.iurl = itemRepo[key].imgUrl;
                articles[i] = article;
            } else {
                articles[i] = undefined;
            }
            i++;
        }
        publisherRepo[publisher] = articles;
    }
}
function setMP(publisher){
    var articles = publisherRepo[publisher];
    if(articles !== undefined){
        var i = 1;
        for(var article in articles){
            setMPContent(i, article);
            i++;
        }
    }
}
function setMPContent(i, article){
    if(article !== undefined){
        setTitle(i, "undefined", "undefined");
        setText(i, "undefined", "undefined");
        setArticleUrl(i, "undefined", "undefined");
        setImgUrl(i, "undefined", "undefined");
    } else{
        setTitle(i, article.title);
        setText(i, article.text);
        setArticleUrl(i, article.url);
        setImgUrl(i, article.iurl);
    }

}
function setTitle(i, titel){
    document.getElementById("h" + "-" + i ).innerHTML = titel;
}
function setText(i, text){
    document.getElementById("t" + "-" + i ).innerHTML = text;
}
function setArticleUrl(i, url){
    document.getElementById("l" + "-" + i ).href = url;
}
function setImgUrl(i, url){
    document.getElementById("i" + "-" + i).src = url;
}
function setRank(i, rank){
    document.getElementById("r" + "-" + i).src = rank;
}
httpGetAsync("/articles", parseJsonItem);
httpGetAsync("/ranking-mp", parseMPRanking);



