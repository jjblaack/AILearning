package com.example.ollamademo.service;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.StreamingResponseHandler;
import dev.langchain4j.model.chat.StreamingChatLanguageModel;
import dev.langchain4j.model.output.Response;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@Service
@RequiredArgsConstructor
public class ChatService {

    private final StreamingChatLanguageModel streamingChatLanguageModel;

    public Flux<String> streamChat(String message) {
        return Flux.create(sink -> {
            // 创建消息处理器
            Consumer<Response<AiMessage>> messageConsumer = response -> {
                String content = response.content().text();
                // 发送消息到客户端
                sink.next(content);
            };

            // 创建完成处理器
            Runnable onComplete = () -> {
                sink.complete();
            };

            try {
                // 发送初始连接成功消息
                sink.next("Connected");
                
                // 创建消息列表
                List<ChatMessage> messages = Collections.singletonList(UserMessage.from(message));
                
                // 创建StreamingResponseHandler
                StreamingResponseHandler<AiMessage> handler = new StreamingResponseHandler<AiMessage>() {
                    @Override
                    public void onNext(String token) {
                        sink.next(token);
                    }
                    
                    @Override
                    public void onComplete(Response<AiMessage> response) {
                        onComplete.run();
                    }
                    
                    @Override
                    public void onError(Throwable error) {
                        // 处理各种可能的错误
                        if (error instanceof NullPointerException) {
                            // 处理空指针异常
                            String errorMsg = error.getMessage() != null ? error.getMessage() : "未知空指针异常";
                            System.err.println("警告: 捕获到空指针异常: " + errorMsg);
                            
                            // 检查是否是getMessage或getContent的空指针异常
                            if (errorMsg.contains("getMessage()") || errorMsg.contains("getContent()")) {
                                sink.next("\n[系统提示: 模型返回了空响应，请重试]");
                                onComplete.run();
                                return;
                            }
                        }
                        
                        try {
                            // 尝试从错误中恢复并继续
                            System.err.println("错误: " + error.getClass().getName() + ": " + error.getMessage());
                            sink.next("\n[系统错误: " + error.getMessage() + "]");
                            onComplete.run();
                        } catch (Exception e) {
                            // 如果恢复失败，则传递错误
                            sink.error(error);
                        }
                    }
                };
                
                // 开始流式处理
                streamingChatLanguageModel.generate(messages, handler);
            } catch (Exception e) {
                sink.error(e);
            }
        }, FluxSink.OverflowStrategy.BUFFER);
    }
}