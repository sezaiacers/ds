#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "ds.hpp"

namespace py = pybind11;

PYBIND11_MODULE(ds, m) {
  m.doc() = "ds";
  pybind11::class_<ds::Dataset>(m, "Dataset")
    .def(pybind11::init<
        std::vector<std::string>,
        std::vector<std::string>,
        size_t,
        size_t,
        size_t,
        size_t,
        size_t>())
    .def("Start", &ds::Dataset::Start)
    .def("Stop", &ds::Dataset::Stop)
    .def("Next", [](ds::Dataset& self) {
        auto example = self.Next();

        std::vector<std::tuple<std::string, py::bytes>> py_example;
        py_example.reserve(example.size());
        for (auto& [name, data, size]: example) {
          py_example.emplace_back(std::move(name), py::bytes(data.get(), size));
        }
        return py_example;
    });
}
