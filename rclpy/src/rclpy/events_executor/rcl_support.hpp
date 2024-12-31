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
#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>

#include <boost/asio/any_io_executor.hpp>

#include <rcl/client.h>
#include <rcl/service.h>
#include <rcl/subscription.h>
#include <rcl/timer.h>
#include <rcl/wait.h>

#include <pybind11/pybind11.h>

#include "scoped_with.hpp"

namespace rclpy {
namespace events_executor {

/// Use this for all RCL event callbacks.  Use the return value from
/// RclCallbackManager::MakeCallback() as the user data arg.
///
/// Note that RCL callbacks are invoked in some arbitrary thread originating from the
/// middleware.  Callbacks should process quickly to avoid blocking the middleware; i.e.
/// all actual work should be posted to an asio loop in another thread.
extern "C" void RclEventCallbackTrampoline(const void* user_data,
                                           size_t number_of_events);

/// Creates and maintains callback wrappers used with the RCL C library.
class RclCallbackManager {
 public:
  /// All user callbacks will be posted on the @p executor given to the constructor.
  /// These callbacks will be invoked without the Python Global Interpreter Lock held,
  /// so if they need to access Python at all make sure to acquire that explicitly.
  explicit RclCallbackManager(const boost::asio::any_io_executor& executor);
  ~RclCallbackManager();

  /// Creates a callback wrapper to be passed to RCL C functions.  @p key should be a
  /// pointer to the rcl object that will be associated with the callback.  @p with
  /// protects the _rclpy object handle owning the RCL object, for the duration the
  /// callback is established.
  const void* MakeCallback(const void* key, std::function<void(size_t)> callback,
                           std::shared_ptr<ScopedWith> with);

  /// Discard a previously constructed callback.  @p key should be the same value
  /// provided to MakeCallback().  Caution: ensure that RCL is no longer using a
  /// callback before invoking this.
  void RemoveCallback(const void* key);

 private:
  /// The C RCL interface deals in raw pointers, so someone needs to own the C++
  /// function objects we'll be calling into.  We use unique pointers so the raw pointer
  /// to the object remains stable while the map is manipulated.
  struct CbEntry {
    std::unique_ptr<std::function<void(size_t)>> cb;
    std::shared_ptr<ScopedWith> with;
  };

  boost::asio::any_io_executor executor_;

  /// The map key is the raw pointer to the RCL entity object (subscription, etc)
  /// associated with the callback.
  std::unordered_map<const void*, CbEntry> owned_cbs_;
};

/// Returns the RCL subscription object pointer from a subscription handle (i.e. the
/// handle attribute of an rclpy Subscription object, which is a _rclpy C++
/// Subscription object).  Assumes that a ScopedWith has already been entered on the
/// given handle.
rcl_subscription_t* GetRclSubscription(pybind11::handle);

/// Returns the RCL timer object pointer from a timer handle (i.e. the handle attribute
/// of an rclpy Timer object, which is a _rclpy C++ Timer object).  Assumes that a
/// ScopedWith has already been entered on the given handle.
rcl_timer_t* GetRclTimer(pybind11::handle);

/// Returns the RCL client object pointer from a client handle (i.e. the handle
/// attribute of an rclpy Client object, which is a _rclpy C++ Client object). Assumes
/// that a ScopedWith has already been entered on the given handle.
rcl_client_t* GetRclClient(pybind11::handle);

/// Returns the RCL service object pointer from a service handle (i.e. the handle
/// attribute of an rclpy Service object, which is a _rclpy C++ Service object).
/// Assumes that a ScopedWith has already been entered on the given handle.
rcl_service_t* GetRclService(pybind11::handle);

/// Returns the RCL wait set object pointer from a _rclpy C++ WaitSet object.  Assumes
/// that a ScopedWith has already been entered on the given object.
rcl_wait_set_t* GetRclWaitSet(pybind11::handle);

}  // namespace events_executor
}  // namespace rclpy