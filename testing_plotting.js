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
                    turboThreshold: 10000
                }
            }
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
    $.getJSON('data.json', function (jsonData) {
         var peers = Object.keys(jsonData);
         peers.forEach(function(peer, index) {
            var data = processPeerData(jsonData[peer]);
            drawChart(data, peer, index);
        });
    });
}

$(initPage);