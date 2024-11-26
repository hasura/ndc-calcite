//
// Created by kennethstott on 11/26/2024.
//

#ifndef JNIPARAM_H
#define JNIPARAM_H

#endif //JNIPARAM_H

#pragma once

#include <jni.h>
#include <string>
#include <vector>

class JniParam {
public:
    enum class Type {
        String,
        StringArray,
        Integer,
        Float,
        Double,
        Boolean
    };

    // Constructors for different types
    explicit JniParam(const std::string& value);
    explicit JniParam(const std::vector<std::string>& value);
    explicit JniParam(int value);
    explicit JniParam(float value);
    explicit JniParam(double value);
    explicit JniParam(bool value);

    JniParam();

    // Get the JNI signature for this type
    std::string getSignature() const;

    // Convert the parameter to JNI value
    jvalue toJValue(JNIEnv* env) const;

    // Clean up any JNI resources
    void cleanup(JNIEnv* env, const jvalue& value) const;

    // Get the parameter type
    [[nodiscard]] Type getType() const { return type_; }
    [[nodiscard]] std::string getString() const { return stringValue_; }
    [[nodiscard]] std::vector<std::string> getStringArray() const { return stringArrayValue_; }
    [[nodiscard]] int getInt() const { return intValue_; }
    [[nodiscard]] float getFloat() const { return floatValue_; }
    [[nodiscard]] double getDouble() const { return doubleValue_; }
    [[nodiscard]] bool getBool() const { return boolValue_; }

private:
    Type type_;
    std::string stringValue_;
    std::vector<std::string> stringArrayValue_;
    int intValue_ = 0;
    float floatValue_ = 0.0f;
    double doubleValue_ = 0.0;
    bool boolValue_ = false;
};