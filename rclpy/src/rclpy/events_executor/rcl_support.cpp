// Copyright 2024 Brad Martin
// Copyright 2024 Merlin Labs, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "events_executor/rcl_support.hpp"

#include <cstdlib>
#include <functional>
#include <utility>

#include <boost/asio/post.hpp>

namespace py = pybind11;

namespace rclpy {
namespace events_executor {

extern "C" void RclEventCallbackTrampoline(const void* user_data,
                                           size_t number_of_events) {
  const auto cb = reinterpret_cast<const std::function<void(size_t)>*>(user_data);
  (*cb)(number_of_events);
}

RclCallbackManager::RclCallbackManager(const boost::asio::any_io_executor& executor)
    : executor_(executor) {}

RclCallbackManager::~RclCallbackManager() {
  // Should not still have any callbacks registered when we exit, because otherwise RCL
  // can call pointers that will no longer be valid.  We can't throw an exception here,
  // but we can explode.
  if (!owned_cbs_.empty()) {
    py::gil_scoped_acquire gil_acquire;
    py::print("Destroying callback manager with callbacks remaining");
    ::abort();
  }
}

const void* RclCallbackManager::MakeCallback(const void* key,
                                             std::function<void(size_t)> callback,
                                             std::shared_ptr<ScopedWith> with) {
  // We don't support replacing an existing callback with a new one, because it gets
  // tricky making sure we don't delete an old callback while the middleware still holds
  // a pointer to it.
  if (owned_cbs_.count(key) != 0) {
    throw py::key_error("Attempt to replace existing callback");
  }
  CbEntry new_entry;
  new_entry.cb = std::make_unique<std::function<void(size_t)>>(
      [this, callback, key](size_t number_of_events) {
        boost::asio::post(executor_, [this, callback, key, number_of_events]() {
          if (!owned_cbs_.count(key)) {
            // This callback has been removed, just drop it as the objects it may want
            // to touch may no longer exist.
            return;
          }
          callback(number_of_events);
        });
      });
  new_entry.with = with;
  const void* ret = new_entry.cb.get();
  owned_cbs_[key] = std::move(new_entry);
  return ret;
}

void RclCallbackManager::RemoveCallback(const void* key) {
  if (!owned_cbs_.erase(key)) {
    throw py::key_error("Attempt to remove nonexistent callback");
  }
}

namespace {
// This helper function is used for retrieving rcl pointers from _rclpy C++ objects.
// Because _rclpy doesn't install its C++ headers for public use, it's difficult to use
// the C++ classes directly.  But, we can treat them as if they are Python objects using
// their defined Python API.  Unfortunately the python interfaces convert the returned
// pointer to integers, so recovering that looks a little weird.
template <typename RclT>
RclT* GetRclPtr(py::handle py_ent_handle) {
  return reinterpret_cast<RclT*>(py::cast<size_t>(py_ent_handle.attr("pointer")));
}
}  // namespace

rcl_subscription_t* GetRclSubscription(py::handle sub_handle) {
  return GetRclPtr<rcl_subscription_t>(sub_handle);
}

rcl_timer_t* GetRclTimer(py::handle timer_handle) {
  return GetRclPtr<rcl_timer_t>(timer_handle);
}

rcl_client_t* GetRclClient(py::handle cl_handle) {
  return GetRclPtr<rcl_client_t>(cl_handle);
}

rcl_service_t* GetRclService(py::handle srv_handle) {
  return GetRclPtr<rcl_service_t>(srv_handle);
}

rcl_wait_set_t* GetRclWaitSet(py::handle ws) { return GetRclPtr<rcl_wait_set_t>(ws); }

}  // namespace events_executor
}  // namespace rclpy