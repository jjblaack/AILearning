package com.example.ollamademo.service;

import dev.langchain4j.model.chat.StreamingChatLanguageModel;
import dev.langchain4j.model.chat.response.StreamingChatResponseHandler;
import dev.langchain4j.model.chat.response.ChatResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Service
@RequiredArgsConstructor
public class ChatService {

    private final StreamingChatLanguageModel streamingChatLanguageModel;

    public Flux<String> streamChat(String message) {
        return Flux.create(sink -> {
            try {
                // 发送初始连接成功消息
                sink.next("Connected");

                // 使用StreamingChatLanguageModel的chat方法
                streamingChatLanguageModel.chat(
                        message,
                        new StreamingChatResponseHandler() {
                            @Override
                            public void onPartialResponse(String partialResponse) {
                                sink.next(partialResponse);
                            }

                            @Override
                            public void onCompleteResponse(ChatResponse completeResponse) {
                                sink.complete();
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
                                        sink.complete();
                                        return;
                                    }
                                }

                                try {
                                    // 尝试从错误中恢复并继续
                                    System.err.println("错误: " + error.getClass().getName() + ": " + error.getMessage());
                                    sink.next("\n[系统错误: " + error.getMessage() + "]");
                                    sink.complete();
                                } catch (Exception e) {
                                    // 如果恢复失败，则传递错误
                                    sink.error(error);
                                }
                            }
                        }
                );
            } catch (Exception e) {
                sink.error(e);
            }
        }, FluxSink.OverflowStrategy.BUFFER);
    }
}
