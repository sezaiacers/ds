#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <regex>
#include <thread>
#include <utility>
#include <vector>

extern "C" {
#include <archive.h>
#include <archive_entry.h>
}

extern "C" {
#include <curl/curl.h>
}

namespace ds {

class Context {
public:
  Context(size_t size, size_t wakeup)
      : buf_{std::make_unique<char[]>(size)}, head_{0}, tail_{0},
        wakeup_{wakeup}, data_{buf_.get()}, size_{0}, finished_{false} {}

  ~Context() {}

  inline size_t Write(char *contents, size_t size, size_t nmemb) {
    size = size * nmemb;

    std::memcpy(buf_.get() + head_, contents, size);

    head_ += size;

    auto diff = head_ - tail_;
    if (diff > wakeup_) {
      {
        std::unique_lock<std::mutex> l(mu_);
        size_ += diff;
      }
      cond_.notify_one();
      tail_ = head_;
    }
    return size;
  }

  inline ssize_t Read(const void **buff) {
    std::unique_lock<std::mutex> l(mu_);
    while (true) {
      if (size_ > 0) {

        auto size = size_;
        size_ = 0;
        l.unlock();

        *buff = data_;
        data_ += size;
        return size;
      } else if (finished_) {
        if (head_ != tail_) {
          auto size = head_ - tail_;
          head_ = tail_;

          *buff = data_;
          return size;
        }
        return 0;
      }
      cond_.wait(l, [this]() { return size_ > 0 || finished_; });
    }
    return 0;
  }

  void Finish() {
    {
      std::unique_lock<std::mutex> l(mu_);
      finished_ = true;
    }
    cond_.notify_one();
  }

private:
  std::unique_ptr<char[]> buf_;
  size_t head_;
  size_t tail_;

  size_t wakeup_;

  char *data_;
  size_t size_;

  bool finished_;

  std::mutex mu_;
  std::condition_variable cond_;
};

static size_t Write(char *contents, size_t size, size_t nmemb, void *userp) {
  return reinterpret_cast<Context *>(userp)->Write(contents, size, nmemb);
}

ssize_t Read(struct archive *, void *userp, const void **buff) {
  return reinterpret_cast<Context *>(userp)->Read(buff);
}

int Close(struct archive *, void *) { return ARCHIVE_OK; }

static size_t AppendStringCallback(char *contents, size_t size, size_t nmemb,
                                   void *userp) {
  size *= nmemb;
  auto str = reinterpret_cast<std::string *>(userp);
  str->append(reinterpret_cast<char *>(contents), size);
  return size;
}

struct curl_slist *ToCurlHeaders(const std::vector<std::string> &hdrs) {
  curl_slist *headers = nullptr;
  for (const auto &hdr : hdrs) {
    headers = curl_slist_append(headers, hdr.c_str());
  }
  return headers;
}

size_t GetBodySize(const std::string &url,
                   const std::vector<std::string> &hdrs) {
  auto curl = curl_easy_init();
  if (!curl) {
    return 0;
  }

  std::string response;

  auto headers = ToCurlHeaders(hdrs);

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "HEAD");

  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, AppendStringCallback);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response);

  auto ret = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  if (ret != CURLE_OK) {
    return 0;
  }

  size_t size = 0;
  std::regex rx(R"(Content-Length:\s*(\d+))", std::regex_constants::icase);

  auto it = std::sregex_iterator(response.begin(), response.end(), rx);
  auto end = std::sregex_iterator();

  while (it != end) {
    auto match = *it;
    size = std::stoll(match[1]);
    ++it;
  }
  return size;
}

class Dataset {
  public:
  using Example = std::vector<std::tuple<std::string, std::unique_ptr<char[]>, size_t>>;

  Dataset(std::vector<std::string> urls, std::vector<std::string> headers,
          size_t shuffle_buffer_size, size_t num_workers, size_t num_blocks,
          size_t num_examples, size_t wakeup)
      : urls_{std::move(urls)}, headers_{std::move(headers)},
        shuffle_buffer_size_{shuffle_buffer_size}, num_workers_{num_workers},
        num_blocks_{num_blocks}, num_examples_{num_examples}, wakeup_{wakeup} {

          shuffled_examples_ = std::make_unique<std::vector<Example>>();
          unshuffled_examples_ = std::make_unique<std::vector<Example>>();
        }

  Example Next() {
    if (shuffled_examples_->size() == 0) {
      {
        std::unique_lock<std::mutex> l(example_mu_);
        if (unshuffled_examples_->size() != shuffle_buffer_size_) {
          example_cond_.wait(l, [this]() {
            return unshuffled_examples_->size() == shuffle_buffer_size_;
          });
        }
        std::swap(shuffled_examples_, unshuffled_examples_);
      }
      example_cond_.notify_one();
      {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(shuffled_examples_->begin(), shuffled_examples_->end(), g);
      }
    }

    auto example = std::move(shuffled_examples_->back());
    shuffled_examples_->pop_back();
    return example;
  }

  void Start() {
    num_workers_ = std::min<size_t>(urls_.size(), num_workers_);
    for (size_t i = 0; i < num_workers_; ++i) {
      workers_.emplace_back(&Dataset::Worker, this);
    }
  }

  void Stop() {


  }
  private:
  void Push(std::vector<Example> examples) {
    while (examples.size() != 0) {
      std::unique_lock<std::mutex> l(example_mu_);
      if (unshuffled_examples_->size() == shuffle_buffer_size_) {
        example_cond_.wait(l, [this]() {
          return unshuffled_examples_->size() != shuffle_buffer_size_;
        });
      }

      auto size = std::min<size_t>(
          shuffle_buffer_size_ - unshuffled_examples_->size(), examples.size());

      auto begin = examples.end() - size;
      auto end = examples.end();

      unshuffled_examples_->insert(unshuffled_examples_->end(),
                                   std::make_move_iterator(begin),
                                   std::make_move_iterator(end));
      l.unlock();
      example_cond_.notify_one();

      examples.erase(begin, end);
    }
  }

private:
  void Worker() {
    while (true) {
      std::string url;
      {
        std::unique_lock<std::mutex> l(workers_mu_);
        if (waiting_to_be_process_urls_.size() == 0) {
          waiting_to_be_process_urls_ = urls_;
          std::random_device rd;
          std::mt19937 g(rd());
          std::shuffle(waiting_to_be_process_urls_.begin(),
                       waiting_to_be_process_urls_.end(), g);
          std::cout << "started new url shuffle" << std::endl;
        }
        url = waiting_to_be_process_urls_.back();
        waiting_to_be_process_urls_.pop_back();
      }
      Collect(url);
    }
  }
  void Collect(std::string url) {
    auto size = GetBodySize(url, headers_);
    std::cout << "start: " << url << ", size: " << size << std::endl;
    if (size == 0) {
      return;
    }

    auto ctx = Context{size, wakeup_};
    std::thread extractor{[&ctx, this]() {
      auto reader = archive_read_new();

      archive_read_support_format_tar(reader);
      archive_read_support_filter_all(reader);

      archive_read_open(reader, &ctx, NULL, Read, Close);

      size_t data_size = 3072;
      auto data = std::make_unique<char[]>(data_size);

      std::vector<Example> examples;
      examples.reserve(num_examples_);
      while (true) {
        bool ok = true;

        Example example;
        example.reserve(num_blocks_);
        for (size_t i = 0; i < num_blocks_; ++i) {
          struct archive_entry *entry;
          auto ret = archive_read_next_header(reader, &entry);
          if (ret != ARCHIVE_OK) {
            ok = false;
            break;
          }
          // could be EOF

          auto name = std::string{archive_entry_pathname(entry)};

          const void *buff;
          size_t size;
          la_int64_t offset;

          size_t total_size = 0;
          while (true) {
            auto ret = archive_read_data_block(reader, &buff, &size, &offset);
            if (ret == ARCHIVE_EOF) {
              break;
            } else if (ret < ARCHIVE_OK) {
              ok = false;
              break;
            }

            total_size = offset + size;
            if (total_size > data_size) {
              data_size = std::max<size_t>(total_size, data_size * 2);

              auto new_data = std::make_unique<char[]>(data_size);
              std::memcpy(new_data.get(), data.get(), offset);
              std::swap(data, new_data);
            }
            std::memcpy(data.get() + offset, buff, size);
          }

          if (!ok) {
            break;
          }

          auto instance = std::make_unique<char[]>(total_size);
          std::memcpy(instance.get(), data.get(), total_size);
          example.emplace_back(std::move(name), std::move(instance), total_size);
        }
        if (!ok) {
          break;
        }

        examples.push_back(std::move(example));
        if (examples.size() == num_examples_) {
          Push(std::move(examples));
          examples.reserve(num_examples_);
        }
      }
      if (examples.size() > 0) {
        Push(std::move(examples));
      }
      archive_read_free(reader);
    }};


    auto curl = curl_easy_init();
    if (!curl) {
      std::cout << "curl init error" << std::endl;
      ctx.Finish();
      extractor.join();
      std::cout << "ended untart finish: " << url << std::endl;
      return;
    }
    auto headers = ToCurlHeaders(headers_);

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "GET");

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, Write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    std::cout << "start downloading " << url << " ...." << std::endl;
    curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    curl_slist_free_all(headers);

    std::cout << "ended downloading " << url << " ...." << std::endl;

    ctx.Finish();
    extractor.join();

    std::cout << "ended untart finish: " << url << std::endl;
  }


private:
  const std::vector<std::string> urls_;
  const std::vector<std::string> headers_;

  size_t shuffle_buffer_size_;

  size_t num_workers_;

  std::unique_ptr<std::vector<Example>> shuffled_examples_;
  std::unique_ptr<std::vector<Example>> unshuffled_examples_;

  std::vector<std::thread> workers_;

  std::vector<std::string> waiting_to_be_process_urls_;

  std::mutex workers_mu_;

  std::mutex example_mu_;
  std::condition_variable example_cond_;

  size_t num_blocks_;
  size_t num_examples_;

  size_t wakeup_;
};

}
