package org.atlasapi.persistence.content.elasticsearch;

import org.elasticsearch.action.ActionListener;

import com.google.common.util.concurrent.SettableFuture;

final class FutureSettingActionListener<T> implements ActionListener<T> {
    private final SettableFuture<T> result;

    FutureSettingActionListener(SettableFuture<T> result) {
        this.result = result;
    }

    @Override
    public void onResponse(T input) {
        this.result.set(input);
    }

    @Override
    public void onFailure(Throwable e) {
        this.result.setException(e);
    }
}