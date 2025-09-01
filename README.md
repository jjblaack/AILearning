# LangChain4j + Ollama + Qwen3:8b 流式输出示例

这是一个使用LangChain4j、Ollama和Qwen3:8b模型实现流式输出的Java SpringBoot示例项目。

## 前提条件

1. Java 17或更高版本
2. Maven
3. 已安装Ollama并下载Qwen3:8b模型

## 安装Ollama和Qwen3:8b模型

1. 安装Ollama: 访问[Ollama官网](https://ollama.ai/)下载并安装
2. 下载Qwen3:8b模型:
   ```bash
   ollama pull qwen3:8b
   ```

## 运行项目

1. 克隆项目到本地
2. 确保Ollama服务正在运行
3. 使用Maven构建并运行项目:
   ```bash
   mvn spring-boot:run
   ```
4. 在浏览器中访问: `http://localhost:8080`

## 项目结构

- `src/main/java/com/example/ollamademo/`
  - `OllamaDemoApplication.java`: 应用程序入口
  - `config/OllamaConfig.java`: Ollama配置类
  - `controller/ChatController.java`: 聊天API控制器
  - `controller/WebController.java`: Web页面控制器
  - `dto/ChatRequest.java`: 聊天请求DTO
  - `service/ChatService.java`: 聊天服务实现

- `src/main/resources/`
  - `application.properties`: 应用配置文件
  - `templates/index.html`: 前端页面

## 技术栈

- SpringBoot 3.2.3
- Spring WebFlux
- LangChain4j 0.27.1
- Thymeleaf
- 响应式编程

## 流式输出实现

本项目使用Spring WebFlux实现从后端到前端的流式输出。主要实现步骤:

1. 后端使用`Flux`创建响应式流
2. LangChain4j的`StreamingChatLanguageModel`提供流式响应
3. 前端使用Fetch API和流式处理接收并显示响应

## 配置说明

在`application.properties`中可以修改以下配置:

```properties
# Ollama配置
ollama.base-url=http://localhost:11434
ollama.model=qwen3:8b
```

## 注意事项

- 确保Ollama服务在运行项目前已启动
- 默认使用的是Qwen3:8b模型，可以在配置中更改为其他支持的模型