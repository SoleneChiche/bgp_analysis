function drawChart(chartData, peer, index) {
    var options = {
            chart: {
                type: 'line'
            },
            title: {
                text: peer + ' : A and W updates'
            },
            xAxis: {
                type: 'datetime',
                labels: {
                rotation: -45
                }
            },
            yAxis: {
                title: {
                    text: 'Number of updates'
                }
            },
            series: chartData,
            plotOptions: {
                series: {
                    turboThreshold: 500000000
                }
            },
            colors: ['#FF0000', '#0000FF']
        };
    
    var container = $('#container');
    var chart = document.createElement('div');
    chart.id = 'chart' + index;
    container.append(chart);
    $('#'+chart.id).highcharts(options);
}

function valuesOfObject(object) {
  var values = [];
  for(var property in object) {
    values.push(object[property]);
  }
  return values;
}

function processPeerData(data){
    var tmp = {};

    for (var key in data) {
        var entry = data[key];
        for (var count in data[key]) {
            if (!(count in tmp)) {
                tmp[count] = {name: count, data: []};
            }
            tmp[count].data.push({x: key*1000, y: entry[count]});
        }
    }
    return valuesOfObject(tmp);
}

function initPage() {
    var file_names;
    file_names = [];
    $.getJSON('json_file_names.json', function (jsonData) {
        file_names = jsonData;
        jsonToGraphs(file_names);
    });
}

function jsonToGraphs(file_names){

var focus = $("<div />");
for(var i = 0; i < file_names.length; i++) {
    (function (i) {
        focus.queue('filenames', function (next) {
            $.getJSON(file_names[i], function(jsonData) {
                var data = processPeerData(jsonData);
                drawChart(data, file_names[i].replace('..','.'), i);
                next();
            });
        });
    })(i);
}
focus.dequeue('filenames');
}

$(initPage);