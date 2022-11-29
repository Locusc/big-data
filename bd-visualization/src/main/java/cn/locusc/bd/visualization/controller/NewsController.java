package cn.locusc.bd.visualization.controller;

import cn.locusc.bd.visualization.service.NewsService;
import com.google.gson.Gson;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

@Controller
public class NewsController {

    @Resource
    private NewsService newsService;

    @RequestMapping("/analysis")
    public void analysis(HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.setContentType("text/html;charset=utf-8");
        PrintWriter pw = httpServletResponse.getWriter();
        pw.write(new Gson().toJson(newsService.execute()));
        pw.flush();
        pw.close();
    }

    @RequestMapping("/page")
    public String page() {
        return "news";
    }

}
