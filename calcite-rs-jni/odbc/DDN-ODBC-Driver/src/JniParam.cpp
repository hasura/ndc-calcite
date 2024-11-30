#include <utility>

#include "../include/JniParam.hpp"
#include "../include/logging.hpp"

JniParam::JniParam(std::string  value)
    : type_(Type::String)
    , stringValue_(std::move(value))
{}

JniParam::JniParam(const std::vector<std::string>& value) 
    : type_(Type::StringArray)
    , stringArrayValue_(value) 
{}

JniParam::JniParam(int value) 
    : type_(Type::Integer)
    , intValue_(value) 
{}

JniParam::JniParam(float value) 
    : type_(Type::Float)
    , floatValue_(value) 
{}

JniParam::JniParam(double value) 
    : type_(Type::Double)
    , doubleValue_(value) 
{}

JniParam::JniParam(bool value) 
    : type_(Type::Boolean)
    , boolValue_(value) 
{}

JniParam::JniParam() = default;

std::string JniParam::getSignature() const {
    switch (type_) {
        case Type::String: 
            return "Ljava/lang/String;";
        case Type::StringArray: 
            return "[Ljava/lang/String;";
        case Type::Integer: 
            return "I";
        case Type::Float: 
            return "F";
        case Type::Double: 
            return "D";
        case Type::Boolean: 
            return "Z";
        default: 
            return "";
    }
}

jvalue JniParam::toJValue(JNIEnv* env) const {
    jvalue val{};
    try {
        switch (type_) {
            case Type::String:
                val.l = stringValue_.empty() ? 
                    nullptr : 
                    env->NewStringUTF(stringValue_.c_str());
                LOGF("Created jstring from: %s", stringValue_.c_str());
                break;
            
            case Type::StringArray: {
                jclass stringClass = env->FindClass("java/lang/String");
                if (!stringClass) {
                    LOG("Failed to find String class");
                    break;
                }

                jobjectArray arr = env->NewObjectArray(
                    stringArrayValue_.size(), stringClass, nullptr);
                if (!arr) {
                    LOG("Failed to create String array");
                    env->DeleteLocalRef(stringClass);
                    break;
                }

                for (size_t i = 0; i < stringArrayValue_.size(); i++) {
                    if (jstring str = env->NewStringUTF(stringArrayValue_[i].c_str())) {
                        env->SetObjectArrayElement(arr, i, str);
                        env->DeleteLocalRef(str);
                    }
                }

                val.l = arr;
                env->DeleteLocalRef(stringClass);
                LOGF("Created String array with %zu elements", stringArrayValue_.size());
                break;
            }
            
            case Type::Integer:
                val.i = intValue_;
                LOGF("Set integer value: %d", intValue_);
                break;
            
            case Type::Float:
                val.f = floatValue_;
                LOGF("Set float value: %f", floatValue_);
                break;
            
            case Type::Double:
                val.d = doubleValue_;
                LOGF("Set double value: %f", doubleValue_);
                break;
            
            case Type::Boolean:
                val.z = boolValue_;
                LOGF("Set boolean value: %d", boolValue_);
                break;
        }
    }
    catch (const std::exception& e) {
        LOGF("Exception in toJValue: %s", e.what());
        // Ensure val is zeroed in case of error
        val = jvalue{};
    }
    return val;
}

void JniParam::cleanup(JNIEnv* env, const jvalue& value) const {
    try {
        if (type_ == Type::String || type_ == Type::StringArray) {
            if (value.l != nullptr) {
                env->DeleteLocalRef(value.l);
                LOG("Cleaned up JNI reference");
            }
        }
    }
    catch (const std::exception& e) {
        LOGF("Exception in cleanup: %s", e.what());
    }
}