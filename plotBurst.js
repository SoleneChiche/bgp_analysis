// Highcharts set up function
function drawChart(chartData, peer, index) {
    // options to configure the highcharts plot
    var options = {
            chart: {
                type: 'line'
            },
            title: {
                text: peer + ' : Burst propagation'
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
            colors: ['#FF0000']
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

function processPeerData(data){
    var tmp = {};
    tmp['burst'] = {name: 'burst', data: []};
    for (var key in data) {
        var entry = data[key];
        tmp['burst'].data.push({x: key*1000, y: entry});
    }
    return valuesOfObject(tmp);
}

// Get the json file names
function initPage() {
    var burst_names;
    burst_names = [];
    $.getJSON('json_burst_cumulative_names.json', function (jsonData) {
        burst_names = jsonData;
        console.log(burst_names);
        jsonToGraphs(burst_names);
    });
}

// Loop over the burst names and asynchronously get the JSON to be processed
function jsonToGraphs(burst_names){
var focus = $("<div />");
for(var i = 0; i < burst_names.length; i++) {
    (function (i) {
        focus.queue('burstnames', function (next) {
            $.getJSON(burst_names[i], function (jsonData) {
                var data = processPeerData(jsonData);
                if(burst_names[i].indexOf('burstW') > -1) {
                drawChart(data, burst_names[i].replace('_', ':'), i);
                }
                next();
            });
        });
    })(i);
}
focus.dequeue('burstnames');
}

$(initPage);