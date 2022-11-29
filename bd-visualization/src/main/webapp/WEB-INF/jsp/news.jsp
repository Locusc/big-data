<%@ page language="java" import="java.util.*" pageEncoding="UTF-8" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort() + path + "/";
%>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
    <script src="../js/echarts.min.js"></script>
    <script src="../js/jquery-3.2.1.js"></script>
</head>
<body>

<div>
    <div id="main" style="width:700px;height: 420px;float:left;">one</div>
    <div id="sum" style="width:730px;height: 420px;float:left;">two</div>
    <div id="period" style="width:1430px;height: 250px;float:left;">three</div>
</div>
<script type="text/javascript">
    var myChart = echarts.init(document.getElementById('main'));
    var myChart_sum = echarts.init(document.getElementById('sum'));
    var myChart_period = echarts.init(document.getElementById('period'));
    $(document).ready(function () {
        initNewsNum();
        setInterval(function () {
            echarts.init(document.getElementById('sum'));
            echarts.init(document.getElementById('main'));
            echarts.init(document.getElementById('period'));
            initNewsNum();
        }, 5000);
    });

    function initNewsNum() {
        var action = "<%=path%>/analysis";
        var $data = $.ajax({url: action, async: false}).responseText;
        var sd = eval('(' + $data + ')')
        newsRank(sd);
        newsSum(sd.newssum);
        periodRank(sd);
    }


    function newsRank(json) {

        var option = {
            backgroundColor: '#ffffff',//背景色
            title: {
                text: '新闻话题浏览量【实时】排行',
                subtext: '数据来自搜狗实验室',
                textStyle: {
                    fontWeight: 'normal',              //标题颜色
                    color: '#408829'
                },
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: ['浏览量']
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: {
                type: 'value',
                boundaryGap: [0, 0.01]
            },
            yAxis: {
                type: 'category',
                data: json.name
            },
            series: [
                {
                    name: '浏览量',
                    type: 'bar',
                    label: {
                        normal: {
                            show: true,
                            position: 'insideRight'
                        }
                    },
                    itemStyle: {normal: {color: '#f47209', size: '50px'}},
                    data: json.newscount
                }

            ]
        };
        myChart.setOption(option);

    }


    function newsSum(data) {

        var option = {
            backgroundColor: '#fbfbfb',//背景色
            title: {
                text: '新闻话题曝光量【实时】统计',
                subtext: '数据来自搜狗实验室'
            },


            tooltip: {
                formatter: "{a} <br/>{b} : {c}%"
            },
            toolbox: {
                feature: {
                    restore: {},
                    saveAsImage: {}
                }
            },
            series: [
                {
                    name: '业务指标',
                    type: 'gauge',
                    max: 50000,
                    detail: {formatter: '{value}个话题'},
                    data: [{value: 50, name: '话题曝光量'}]
                }
            ]
        };

        option.series[0].data[0].value = data;
        myChart_sum.setOption(option, true);

    }

    function periodRank(json) {
        option = {
            backgroundColor: '#ffffff',//背景色
            color: ['#00FFFF'],
            tooltip: {
                trigger: 'axis',
                axisPointer: {            // 坐标轴指示器，坐标轴触发有效
                    type: 'shadow'        // 默认为直线，可选为：'line' | 'shadow'
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: [
                {
                    type: 'category',
                    data: json.logtime,
                    axisTick: {
                        alignWithLabel: true
                    }
                }
            ],
            yAxis: [
                {
                    type: 'value'
                }
            ],
            series: [
                {
                    name: '新闻话题曝光量',
                    type: 'bar',
                    barWidth: '60%',
                    data: json.periodcount
                }
            ]
        };
        myChart_period.setOption(option, true);
    }
</script>
</body>
</html>
