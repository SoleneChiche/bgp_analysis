// Highcharts set up function
function drawChart(chartData, peer, index) {
    // options to configure the highcharts plot
    var options = {
            chart: {
                type: 'line'
            },
            title: {
                text: peer + ' : A and W updates'
            },
            xAxis: {
                type: 'datetime',
                title: {
                    text: 'Date'
                },
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
                    turboThreshold: 500000000  // Number of point the plot can handle
                }
            },
            colors: ['#FF0000', '#0000FF']
        };

    // Append the graph to a new div container
    var container = $('#container');
    var chart = document.createElement('div');
    chart.id = 'chart' + index;
    container.append(chart);
    $('#'+chart.id).highcharts(options);
}

// Helper to get the value of an object
function valuesOfObject(object) {
  var values = [];
  for(var property in object) {
    values.push(object[property]);
  }
  return values;
}

// Transform the data {timestamp ==> x : {'A':..., 'W':... ==> y}}
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

// Get the json file names
function initPage() {
    var file_names;
    file_names = [];
    $.getJSON('json_file_names-rrc.json', function (jsonData) {
        file_names = jsonData;
        jsonToGraphs(file_names);
    });
}

// Loop over the filenames and asynchronously get the JSON to be processed
function jsonToGraphs(file_names){

var focus = $("<div />");
for(var i = 0; i < file_names.length; i++) {
    (function (i) {
        focus.queue('filenames', function (next) {
            $.getJSON(file_names[i], function(jsonData) {
                var data = processPeerData(jsonData);
                drawChart(data, file_names[i].replace('_',':'), i);
                next();
            });
        });
    })(i);
}
focus.dequeue('filenames');
}

$(initPage);