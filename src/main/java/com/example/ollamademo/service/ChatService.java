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
                
                // 用于跟踪思考状态的变量
                final boolean[] insideThinkingBlock = {false};
                final StringBuilder[] thinkingContent = {new StringBuilder()};
                
                // 创建StreamingResponseHandler
                StreamingResponseHandler<AiMessage> handler = new StreamingResponseHandler<AiMessage>() {
                    @Override
                    public void onNext(String token) {
                        // 检测思考块的开始
                        if (token.contains("<think>")) {
                            insideThinkingBlock[0] = true;
                            // 发送不包含<think>标签的部分
                            String[] parts = token.split("<think>", 2);
                            if (parts.length > 0 && !parts[0].isEmpty()) {
                                sink.next(parts[0]);
                            }
                            
                            // 发送思考块开始标记
                            sink.next("\n<details><summary>思考过程（点击展开）</summary>\n\n");
                            
                            // 如果分割后还有内容，添加到思考内容中
                            if (parts.length > 1) {
                                thinkingContent[0].append(parts[1]);
                            }
                            return;
                        }
                        
                        // 检测思考块的结束
                        if (token.contains("</think>")) {
                            insideThinkingBlock[0] = false;
                            // 处理思考块结束前的内容
                            String[] parts = token.split("</think>", 2);
                            if (parts.length > 0) {
                                thinkingContent[0].append(parts[0]);
                            }
                            
                            // 发送累积的思考内容
                            sink.next(thinkingContent[0].toString());
                            thinkingContent[0] = new StringBuilder(); // 重置思考内容
                            
                            // 发送思考块结束标记
                            sink.next("\n</details>\n\n");
                            
                            // 发送思考块后的内容
                            if (parts.length > 1 && !parts[1].isEmpty()) {
                                sink.next(parts[1]);
                            }
                            return;
                        }
                        
                        // 如果在思考块内，累积思考内容
                        if (insideThinkingBlock[0]) {
                            thinkingContent[0].append(token);
                        } else {
                            // 不在思考块内，直接发送token
                            sink.next(token);
                        }
                    }
                    
                    @Override
                    public void onComplete(Response<AiMessage> response) {
                        onComplete.run();
                    }
                    
                    @Override
                    public void onError(Throwable error) {
                        sink.error(error);
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