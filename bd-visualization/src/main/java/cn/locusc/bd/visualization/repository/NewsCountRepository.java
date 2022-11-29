package cn.locusc.bd.visualization.repository;

import cn.locusc.bd.visualization.domain.NewsCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author Jay
 * 类型统计数据接口
 * 2022/11/28
 */
@Repository
public interface NewsCountRepository extends JpaRepository<NewsCount, String> {

    @Query(value = "select name,count from newscount order by count desc limit 10", nativeQuery = true)
    List<NewsCount> newsRank();

}
