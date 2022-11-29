package cn.locusc.bd.visualization.service.impl;

import cn.locusc.bd.visualization.domain.NewsCount;
import cn.locusc.bd.visualization.domain.PeriodCount;
import cn.locusc.bd.visualization.repository.NewsCountRepository;
import cn.locusc.bd.visualization.repository.PeriodCountRepository;
import cn.locusc.bd.visualization.service.NewsService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jay
 * 新闻信息接口实现
 * 2022/11/28
 */
@Service
public class NewsServiceImpl implements NewsService {

    @Resource
    private NewsCountRepository newsCountRepository;


    @Resource
    private PeriodCountRepository periodCountRepository;

    /**
     * 查询新闻统计信息
     * @return java.util.Map<java.lang.String,java.lang.Object>
     */
    @Override
    public Map<String, Object> execute() {
        Map<String, Object> map = new HashMap<>();

        long newsCount = newsCountRepository.count();

        List<NewsCount> newsCounts = newsCountRepository.newsRank();
        // 新闻浏览量排行
        map.put("name", newsCounts.stream().map(NewsCount::getName).toArray());
        map.put("newscount", newsCounts.stream().map(NewsCount::getCount).toArray());

        List<PeriodCount> periodCounts = periodCountRepository.periodRank();
        // 新闻时段浏览量排行
        map.put("logtime", periodCounts.stream().map(PeriodCount::getLogTime).toArray());
        map.put("periodcount", periodCounts.stream().map(PeriodCount::getCount).toArray());

        // 新闻话题总量
        map.put("newssum", newsCount);
        return map;
    }
}
