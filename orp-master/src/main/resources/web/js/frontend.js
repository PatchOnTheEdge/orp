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
    var items = JSON.parse(responseText).items;

    for(i in items){
        var itemId = items[i].itemId;
        var item = items[i].item;
        itemRepo[itemId] = item;
    }
}
function parseMPRanking(responseText){
    var rankings = JSON.parse(responseText).rankings;

    for(p in rankings){
        var publisher = rankings[p].publisherId;
        var ranking = rankings[p].ranking;
        var articles = {};
        var i = 1;
        for(r in ranking){
            //if(i == 3){break;}
            var key = ranking[r].key;
            var rank = ranking[r].rank;


            var article = {};
            if(key in itemRepo){
                article.rank = rank;
                article.title = itemRepo[key].title;
                article.kicker = itemRepo[key].kicker;
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
function setPublisher(publisher) {
    switch (publisher){
        case "1677": document.getElementById("publisher").innerHTML = "Tagesspiegel"; break;
        case "596": document.getElementById("publisher").innerHTML = "Sport1"; break;
        case "418": document.getElementById("publisher").innerHTML = "K&ouml;lner Stadtanzeiger"; break;
        case "694": document.getElementById("publisher").innerHTML = "Motortalk"; break;
        case "35774" : document.getElementById("publisher").innerHTML = "Gulli"; break;
        case "13554" : document.getElementById("publisher").innerHTML = "Cio"; break;
    }
}
function setMP(publisher){
    setPublisher(publisher);
    var articles = publisherRepo[publisher];

    if(articles !== undefined){
        for(i in articles){
            if(articles[i] != undefined){
                console.log(i + articles[i].title);
                setMPContent(i, articles[i]);
            }
        }
    } else {
        console.log("No Articles for publisher with id = " + publisher);
    }
}
function setMPContent(i, article){
    if(article == undefined){
        setTitle(i, "undefined");
        setText(i, "undefined");
        setArticleUrl(i, "undefined");
        setImgUrl(i, "undefined");
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
//console.log(itemRepo[1]);


